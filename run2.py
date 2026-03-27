#!/usr/bin/env python3
"""
LocalChat v3.0 — chat over LAN or internet via playit.gg
Pure Python stdlib + auto-downloaded playit agent
Usage: python run2.py
"""

import socket
import threading
import sqlite3
import json
import hashlib
import os
import sys
import time
import random
import platform
import subprocess
import urllib.request
import urllib.error
import re
import signal
import errno
from datetime import datetime

# ─── Config ────────────────────────────────────────────────────────────────────
HOST_PORT         = 9999
MAX_MEMBERS       = 5          # host + 4 members
BUFFER            = 4096
DB_FILE           = "chat.db"
CONNECT_TIMEOUT   = 20
HANDSHAKE_TIMEOUT = 120
PLAYIT_DIR        = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".playit")
PLAYIT_TOML       = os.path.join(PLAYIT_DIR, "playit.toml")
TUNNEL_ADDR_FILE  = os.path.join(PLAYIT_DIR, "tunnel_address.txt")

# ─── ANSI colours ──────────────────────────────────────────────────────────────
def _supports_ansi():
    if sys.platform == "win32":
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
            return True
        except Exception:
            return bool(os.environ.get("WT_SESSION") or os.environ.get("TERM"))
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

_ANSI = _supports_ansi()
def _a(c): return f"\033[{c}m" if _ANSI else ""

C_RESET  = _a("0");  C_BOLD  = _a("1");  C_DIM   = _a("2")
C_CYAN   = _a("96"); C_YEL   = _a("93"); C_GREEN = _a("92")
C_RED    = _a("91"); C_MAG   = _a("95"); C_BLUE  = _a("94")
C_WHITE  = _a("97")
MEMBER_COLOURS = [C_CYAN, C_YEL, C_GREEN, C_MAG, C_BLUE]

# ─── Banner ────────────────────────────────────────────────────────────────────
def banner():
    print(f"""
{C_BOLD}{C_CYAN}╔══════════════════════════════════════╗
║         LocalChat  v3.0              ║
║  Chat on LAN or internet via playit  ║
╚══════════════════════════════════════╝{C_RESET}
""")

# ─── Safe helpers ──────────────────────────────────────────────────────────────
def gen_passkey(n=6):
    return "".join(random.choices("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", k=n))

def hash_passkey(pk):
    return hashlib.sha256(pk.upper().encode()).hexdigest()

def now_str():
    return datetime.now().strftime("%H:%M:%S")

def input_safe(prompt=""):
    """Read a line from stdin. Exits gracefully on Ctrl+C / EOF."""
    try:
        return input(prompt).strip()
    except (KeyboardInterrupt, EOFError):
        print(f"\n{C_DIM}Bye!{C_RESET}")
        sys.exit(0)

def choice_prompt(prompt, valid):
    """Keep asking until user gives a valid choice."""
    while True:
        c = input_safe(prompt)
        if c in valid:
            return c
        print(f"{C_RED}Please enter one of: {', '.join(valid)}{C_RESET}")

def get_lan_ip():
    """Best-effort LAN IP detection."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(2)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        # Fallback: iterate interfaces
        try:
            hostname = socket.gethostname()
            return socket.gethostbyname(hostname)
        except Exception:
            return "127.0.0.1"

def check_port_available(port):
    """Return True if local port is free."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("0.0.0.0", port))
        s.close()
        return True
    except OSError:
        return False

def check_internet():
    """Quick connectivity check."""
    try:
        urllib.request.urlopen("https://api.playit.gg", timeout=5)
        return True
    except Exception:
        return False

# ─── playit.gg binary download ─────────────────────────────────────────────────
# Maps (os, arch) → download URL
PLAYIT_URLS = {
    ("linux",   "x86_64"): "https://github.com/playit-cloud/playit-agent/releases/latest/download/playit-linux-amd64",
    ("linux",   "arm64"):  "https://github.com/playit-cloud/playit-agent/releases/latest/download/playit-linux-aarch64",
    ("linux",   "armv7l"): "https://github.com/playit-cloud/playit-agent/releases/latest/download/playit-linux-armv7",
    ("darwin",  "x86_64"): "https://github.com/playit-cloud/playit-agent/releases/latest/download/playit-darwin-amd64",
    ("darwin",  "arm64"):  "https://github.com/playit-cloud/playit-agent/releases/latest/download/playit-darwin-aarch64",
    ("windows", "x86_64"): "https://github.com/playit-cloud/playit-agent/releases/latest/download/playit-windows-amd64.exe",
    ("windows", "x86"):    "https://github.com/playit-cloud/playit-agent/releases/latest/download/playit-windows-386.exe",
}

def _normalise_machine(m):
    m = m.lower()
    if m in ("amd64", "x86_64", "x64"):        return "x86_64"
    if m in ("arm64", "aarch64"):               return "arm64"
    if m in ("armv7l", "armv6l", "arm"):        return "armv7l"
    if m in ("i386", "i686", "x86"):            return "x86"
    return m

def _playit_bin():
    name = "playit.exe" if sys.platform == "win32" else "playit"
    return os.path.join(PLAYIT_DIR, name)

def _ensure_playit():
    """Download playit binary if missing. Returns path or None."""
    binary = _playit_bin()
    if os.path.isfile(binary) and os.access(binary, os.X_OK):
        return binary

    os_name = platform.system().lower()       # linux / darwin / windows
    machine = _normalise_machine(platform.machine())
    key     = (os_name, machine)

    # Try fallbacks for exotic arches
    if key not in PLAYIT_URLS:
        for fallback in [(os_name, "x86_64"), (os_name, "arm64")]:
            if fallback in PLAYIT_URLS:
                print(f"{C_YEL}No exact build for {machine}, falling back to {fallback[1]}.{C_RESET}")
                key = fallback
                break

    if key not in PLAYIT_URLS:
        print(f"{C_RED}No playit build found for {os_name}/{machine}.{C_RESET}")
        print(f"{C_DIM}Download manually from https://playit.gg/download and place in:{C_RESET}")
        print(f"{C_DIM}  {PLAYIT_DIR}{C_RESET}")
        return None

    url = PLAYIT_URLS[key]
    os.makedirs(PLAYIT_DIR, exist_ok=True)

    print(f"{C_DIM}Downloading playit agent ({os_name}/{machine})…{C_RESET}", flush=True)

    # Check internet before trying
    if not check_internet():
        print(f"{C_RED}No internet connection. Cannot download playit.{C_RESET}")
        return None

    tmp = binary + ".tmp"
    try:
        def _progress(count, block, total):
            if total > 0:
                pct = min(100, count * block * 100 // total)
                bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                print(f"\r  [{bar}] {pct}%", end="", flush=True)

        urllib.request.urlretrieve(url, tmp, reporthook=_progress)
        print()  # newline after progress bar
    except urllib.error.HTTPError as e:
        print(f"\n{C_RED}HTTP {e.code} downloading playit: {e.reason}{C_RESET}")
        _safe_remove(tmp)
        return None
    except urllib.error.URLError as e:
        print(f"\n{C_RED}Network error: {e.reason}{C_RESET}")
        _safe_remove(tmp)
        return None
    except Exception as e:
        print(f"\n{C_RED}Download failed: {e}{C_RESET}")
        _safe_remove(tmp)
        return None

    # Verify the file is not empty / an HTML error page
    try:
        size = os.path.getsize(tmp)
        if size < 1024:
            print(f"{C_RED}Downloaded file is too small ({size} bytes) — likely an error page.{C_RESET}")
            _safe_remove(tmp)
            return None
    except OSError:
        pass

    try:
        os.replace(tmp, binary)
        os.chmod(binary, 0o755)
    except OSError as e:
        print(f"{C_RED}Could not install playit binary: {e}{C_RESET}")
        _safe_remove(tmp)
        return None

    print(f"{C_GREEN}playit downloaded successfully.{C_RESET}")
    return binary

def _safe_remove(path):
    try:
        os.remove(path)
    except OSError:
        pass

# ─── Saved tunnel address ──────────────────────────────────────────────────────
def _read_saved_tunnel():
    """Read saved tunnel address. Returns (host, port) or (None, None)."""
    if not os.path.isfile(TUNNEL_ADDR_FILE):
        return None, None
    try:
        data = open(TUNNEL_ADDR_FILE).read().strip()
        if ":" in data:
            host, port_s = data.rsplit(":", 1)
            host = host.strip()
            port = int(port_s.strip())
            if host and 1 <= port <= 65535:
                return host, port
    except (ValueError, OSError):
        pass
    return None, None

def _save_tunnel(host, port):
    """Persist tunnel address for future runs."""
    os.makedirs(PLAYIT_DIR, exist_ok=True)
    try:
        with open(TUNNEL_ADDR_FILE, "w") as f:
            f.write(f"{host}:{port}\n")
    except OSError as e:
        print(f"{C_YEL}Warning: could not save tunnel address: {e}{C_RESET}")

def _clear_saved_tunnel():
    _safe_remove(TUNNEL_ADDR_FILE)

# ─── Parse a host:port tunnel address ─────────────────────────────────────────
def parse_tunnel_address(raw):
    """
    Accept:  host:port   or   [ipv6]:port
    Returns: (host_str, port_int)  or raises ValueError.
    """
    raw = raw.strip()
    if not raw:
        raise ValueError("Empty input.")

    # IPv6: [::1]:1234
    ipv6_match = re.match(r"^\[(.+)\]:(\d+)$", raw)
    if ipv6_match:
        host = ipv6_match.group(1)
        port = int(ipv6_match.group(2))
    elif ":" in raw:
        # Could be host:port or bare IPv6 — count colons
        parts = raw.rsplit(":", 1)
        if len(parts) == 2:
            host, port_s = parts
            try:
                port = int(port_s)
            except ValueError:
                raise ValueError(f"Port must be a number, got: {port_s!r}")
        else:
            raise ValueError("Cannot parse address — use  host:port  format.")
    else:
        raise ValueError("No port found — use  host:port  format.")

    if not host:
        raise ValueError("Host cannot be empty.")
    if not (1 <= port <= 65535):
        raise ValueError(f"Port {port} is out of range (1-65535).")
    return host, port

# ─── Start / manage playit process ────────────────────────────────────────────
_playit_proc = None   # global so we can kill it on shutdown

def _kill_playit():
    global _playit_proc
    if _playit_proc and _playit_proc.poll() is None:
        try:
            _playit_proc.terminate()
            _playit_proc.wait(timeout=3)
        except Exception:
            try:
                _playit_proc.kill()
            except Exception:
                pass
    _playit_proc = None

def _start_playit_background(binary):
    """Launch playit agent silently in background. Returns proc or None."""
    global _playit_proc
    cmd = [binary, "--secret_path", PLAYIT_TOML]
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _playit_proc = proc
        threading.Thread(target=proc.wait, daemon=True).start()
        return proc
    except OSError as e:
        print(f"{C_RED}Could not start playit agent: {e}{C_RESET}")
        return None

def _wait_for_claim_url(binary, timeout=90):
    """
    Run playit in foreground, capture its output, and return the claim URL
    once it appears. Returns URL string or None on timeout/error.
    """
    cmd = [binary, "--secret_path", PLAYIT_TOML, "--stdout"]
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
    except OSError as e:
        print(f"{C_RED}Could not launch playit: {e}{C_RESET}")
        return None

    deadline = time.time() + timeout
    claim_url = None

    while time.time() < deadline:
        try:
            proc.stdout.readable()  # will raise if pipe closed
            raw = proc.stdout.readline()
        except Exception:
            break

        if not raw:
            # Process ended
            rc = proc.poll()
            if rc is not None:
                print(f"{C_RED}playit exited unexpectedly (code {rc}).{C_RESET}")
            break

        line = raw.decode(errors="replace").strip()

        # Claim URL pattern
        m = re.search(r'(https://playit\.gg/claim/[A-Za-z0-9_\-]+)', line)
        if m:
            claim_url = m.group(1)
            break

        # Already claimed / already connected
        if any(kw in line.lower() for kw in ("secret written", "connected", "running")):
            # No claim URL needed
            claim_url = "__already_claimed__"
            break

        # Fatal errors
        if any(kw in line.lower() for kw in ("permission denied", "fatal", "error")):
            print(f"{C_RED}playit error: {line}{C_RESET}")

    proc.terminate()
    try:
        proc.wait(timeout=3)
    except Exception:
        pass

    return claim_url

# ─── Main tunnel entry point ───────────────────────────────────────────────────
def start_tunnel(local_port):
    """
    Set up a playit.gg TCP tunnel.

    Flow:
      A) Binary missing → download it
      B) Saved tunnel address exists → offer to reuse (start agent silently)
      C) No secret (first run) → claim flow → user pastes tunnel address
      D) Secret exists but no saved address → agent starts, user pastes address
      E) Any step fails → fall back to LAN-only gracefully

    Returns (public_host, public_port) or (None, None).
    """
    print(f"\n{C_DIM}Setting up internet tunnel via playit.gg…{C_RESET}")

    # ── Step 1: ensure binary ──────────────────────────────────────────────────
    binary = _ensure_playit()
    if not binary:
        _tunnel_fallback_hint()
        return None, None

    # ── Step 2: check if we have a saved address ───────────────────────────────
    saved_host, saved_port = _read_saved_tunnel()
    if saved_host and saved_port:
        print(f"\n{C_DIM}Previously used tunnel:{C_RESET} {C_YEL}{C_BOLD}{saved_host}:{saved_port}{C_RESET}")
        ans = choice_prompt(
            f"Use it again? (Y = yes / N = enter new address / D = delete & re-setup): ",
            ("y", "Y", "n", "N", "d", "D", "")
        ).lower()

        if ans in ("y", ""):
            # Start agent in background and return saved address
            proc = _start_playit_background(binary)
            if proc is None:
                return None, None
            time.sleep(2)
            if proc.poll() is not None:
                print(f"{C_RED}playit agent exited immediately. Secret may be invalid.{C_RESET}")
                _offer_reset_secret()
                return None, None
            print(f"{C_GREEN}✓ Tunnel active: {saved_host}:{saved_port}{C_RESET}")
            return saved_host, saved_port

        elif ans == "d":
            _clear_saved_tunnel()
            _safe_remove(PLAYIT_TOML)
            print(f"{C_DIM}Cleared saved tunnel and secret. Starting fresh…{C_RESET}")
            saved_host, saved_port = None, None
            # Fall through to claim flow

        # ans == "n": fall through to ask for a new address

    # ── Step 3: determine if we need to claim (first run) ─────────────────────
    need_claim = not os.path.isfile(PLAYIT_TOML)

    if need_claim:
        print(f"\n{C_YEL}{C_BOLD}── playit.gg first-time setup ──{C_RESET}\n")
        print(f"{C_DIM}playit.gg is free and requires no credit card.{C_RESET}")
        print(f"{C_DIM}Starting agent to get your claim URL…{C_RESET}\n")

        claim_url = _wait_for_claim_url(binary, timeout=60)

        if claim_url is None:
            print(f"{C_RED}Could not get claim URL from playit agent.{C_RESET}")
            _tunnel_fallback_hint()
            return None, None

        if claim_url != "__already_claimed__":
            _print_claim_instructions(claim_url, local_port)
            input_safe("Press Enter once you have completed the steps above…")
        else:
            print(f"{C_GREEN}Agent already claimed — skipping claim step.{C_RESET}")

        # Start agent properly now that it has its secret
        proc = _start_playit_background(binary)
        if proc is None:
            return None, None
        time.sleep(3)

        if proc.poll() is not None:
            rc = proc.returncode
            print(f"{C_RED}playit agent exited (code {rc}) after claim.{C_RESET}")
            print(f"{C_DIM}Try deleting {PLAYIT_TOML} and running again.{C_RESET}")
            return None, None

    else:
        # Secret exists, agent not running yet — start it
        print(f"{C_DIM}Starting playit agent (secret found)…{C_RESET}")
        proc = _start_playit_background(binary)
        if proc is None:
            return None, None
        time.sleep(3)

        if proc.poll() is not None:
            rc = proc.returncode
            print(f"{C_RED}playit agent exited immediately (code {rc}).{C_RESET}")
            _offer_reset_secret()
            return None, None

        print(f"{C_GREEN}Agent running.{C_RESET}")
        if not saved_host:
            print(f"\n{C_DIM}Please open https://playit.gg and go to your tunnel dashboard{C_RESET}")
            print(f"{C_DIM}to check your tunnel's public address if you don't have it.{C_RESET}")

    # ── Step 4: get tunnel address from user ───────────────────────────────────
    host, port = _prompt_tunnel_address(local_port, prefill_host=saved_host, prefill_port=saved_port)
    if host is None:
        _kill_playit()
        return None, None

    _save_tunnel(host, port)
    print(f"\n{C_GREEN}{C_BOLD}✓ Internet tunnel ready: {host}:{port}{C_RESET}")
    return host, port

def _print_claim_instructions(claim_url, local_port):
    print(f"""
{C_BOLD}Follow these steps to create your free tunnel:{C_RESET}

  {C_BOLD}1.{C_RESET} Open this URL in your browser:
     {C_YEL}{C_BOLD}{claim_url}{C_RESET}

  {C_BOLD}2.{C_RESET} {C_DIM}Click "Sign in as guest" (no account needed) OR log in{C_RESET}

  {C_BOLD}3.{C_RESET} {C_DIM}The page will ask you to name your agent — enter anything{C_RESET}

  {C_BOLD}4.{C_RESET} {C_DIM}Go to "Tunnels" → "Add Tunnel" → select "Custom TCP"{C_RESET}

  {C_BOLD}5.{C_RESET} {C_DIM}Set these values:{C_RESET}
       Local IP:   127.0.0.1
       Local Port: {C_YEL}{local_port}{C_RESET}

  {C_BOLD}6.{C_RESET} {C_DIM}Click "Add Tunnel" — you'll see a public address like:{C_RESET}
       {C_YEL}abc123.at.ply.gg:47234{C_RESET}  ← copy this exactly
""")

def _prompt_tunnel_address(local_port, prefill_host=None, prefill_port=None, max_tries=5):
    """
    Ask user to paste their tunnel address.
    Returns (host, port) or (None, None) if user skips.
    """
    if prefill_host and prefill_port:
        hint = f"{C_DIM}(previous: {prefill_host}:{prefill_port}){C_RESET} "
    else:
        hint = ""

    print(f"\n{C_DIM}What is your playit tunnel public address?{C_RESET}")
    print(f"{C_DIM}Format: hostname:port   e.g.  abc.at.ply.gg:47234{C_RESET}\n")

    for attempt in range(1, max_tries + 1):
        raw = input_safe(f"Tunnel address {hint}(or 'skip' to use LAN only): ")

        if raw.lower() in ("skip", "s", ""):
            print(f"{C_YEL}Skipping internet tunnel. LAN mode only.{C_RESET}")
            return None, None

        try:
            host, port = parse_tunnel_address(raw)
            return host, port
        except ValueError as e:
            remaining = max_tries - attempt
            if remaining > 0:
                print(f"{C_RED}  ✗ {e}  ({remaining} tries left){C_RESET}")
            else:
                print(f"{C_RED}  ✗ {e}  Giving up — LAN mode only.{C_RESET}")

    return None, None

def _offer_reset_secret():
    print(f"\n{C_YEL}Your saved secret may be invalid or expired.{C_RESET}")
    ans = choice_prompt("Delete secret and start fresh? (y/n): ", ("y", "Y", "n", "N")).lower()
    if ans == "y":
        _safe_remove(PLAYIT_TOML)
        _clear_saved_tunnel()
        print(f"{C_DIM}Deleted. Run the program again to re-claim.{C_RESET}")

def _tunnel_fallback_hint():
    print(f"\n{C_DIM}Internet tunnel unavailable — you can still use LAN mode.{C_RESET}")
    print(f"{C_DIM}For manual setup visit: https://playit.gg/download{C_RESET}")

# ─── Database ──────────────────────────────────────────────────────────────────
class Database:
    def __init__(self, path):
        self.path   = path
        self._local = threading.local()
        self._init()

    def _conn(self):
        if not hasattr(self._local, "c") or self._local.c is None:
            c = sqlite3.connect(self.path, check_same_thread=False, timeout=10)
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=NORMAL")
            c.execute("PRAGMA foreign_keys=ON")
            self._local.c = c
        return self._local.c

    def _init(self):
        try:
            with sqlite3.connect(self.path) as c:
                c.executescript("""
                    PRAGMA journal_mode=WAL;
                    PRAGMA foreign_keys=ON;
                    CREATE TABLE IF NOT EXISTS rooms (
                        id          INTEGER PRIMARY KEY,
                        passkey     TEXT    UNIQUE NOT NULL,
                        host_name   TEXT    NOT NULL,
                        created     TEXT    NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS messages (
                        id       INTEGER PRIMARY KEY,
                        room_id  INTEGER NOT NULL,
                        sender   TEXT    NOT NULL,
                        body     TEXT    NOT NULL,
                        ts       TEXT    NOT NULL,
                        FOREIGN KEY(room_id) REFERENCES rooms(id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_msg_room ON messages(room_id);
                """)
        except sqlite3.Error as e:
            print(f"{C_RED}Database init error: {e}{C_RESET}")
            sys.exit(1)

    def create_room(self, pk_hash, host_name):
        try:
            c = self._conn()
            cur = c.execute(
                "INSERT INTO rooms (passkey,host_name,created) VALUES (?,?,?)",
                (pk_hash, host_name, datetime.now().isoformat())
            )
            c.commit()
            return cur.lastrowid
        except sqlite3.Error as e:
            print(f"{C_RED}DB error creating room: {e}{C_RESET}")
            sys.exit(1)

    def save_message(self, room_id, sender, body):
        try:
            c = self._conn()
            c.execute(
                "INSERT INTO messages (room_id,sender,body,ts) VALUES (?,?,?,?)",
                (room_id, sender, body, datetime.now().isoformat())
            )
            c.commit()
        except sqlite3.Error as e:
            # Non-fatal: log and continue
            print(f"{C_YEL}Warning: could not save message: {e}{C_RESET}")

    def get_history(self, room_id, limit=50):
        try:
            rows = self._conn().execute(
                "SELECT sender,body,ts FROM messages "
                "WHERE room_id=? ORDER BY id DESC LIMIT ?",
                (room_id, limit)
            ).fetchall()
            return list(reversed(rows))
        except sqlite3.Error:
            return []

# ─── Protocol helpers ──────────────────────────────────────────────────────────
def send_msg(sock, **kw):
    """Serialize kw as JSON and send over sock. Silently ignores broken pipes."""
    try:
        data = (json.dumps(kw) + "\n").encode()
        # sendall can raise BrokenPipeError, ConnectionResetError, etc.
        sock.sendall(data)
        return True
    except (OSError, BrokenPipeError):
        return False

def recv_msg(sock, buf=b""):
    """
    Read one newline-delimited JSON message from sock.
    Returns (parsed_dict_or_None, remaining_buf).
    Returns (None, buf) on socket error or malformed JSON.
    """
    while b"\n" not in buf:
        try:
            chunk = sock.recv(BUFFER)
            if not chunk:
                return None, buf      # connection closed cleanly
            buf += chunk
        except socket.timeout:
            return None, buf
        except OSError:
            return None, buf

    line, rest = buf.split(b"\n", 1)
    try:
        return json.loads(line.decode(errors="replace")), rest
    except (json.JSONDecodeError, UnicodeDecodeError):
        # Discard malformed frame, keep buffer
        return None, rest

# ─── Server ────────────────────────────────────────────────────────────────────
class ChatServer:
    def __init__(self, host_name, passkey, db):
        self.host_name   = host_name
        self.passkey     = passkey
        self.pk_hash     = hash_passkey(passkey)
        self.db          = db
        self.room_id     = db.create_room(self.pk_hash, host_name)
        self.clients     = {}        # sock → {name, colour}
        self.lock        = threading.Lock()
        self.colour_pool = list(MEMBER_COLOURS)
        random.shuffle(self.colour_pool)
        self._srv_sock   = None

    # ── colour management ──────────────────────────────────────────────────────
    def _alloc_colour(self):
        with self.lock:
            return self.colour_pool.pop(0) if self.colour_pool else C_WHITE

    def _free_colour(self, colour):
        with self.lock:
            if colour not in self.colour_pool:
                self.colour_pool.append(colour)

    # ── broadcast ──────────────────────────────────────────────────────────────
    def broadcast(self, payload, exclude=None):
        dead = []
        with self.lock:
            targets = list(self.clients.keys())
        for s in targets:
            if s is exclude:
                continue
            if not send_msg(s, **payload):
                dead.append(s)
        for s in dead:
            self._drop(s, reason="send_failed")

    # ── drop a client ──────────────────────────────────────────────────────────
    def _drop(self, sock, reason=""):
        with self.lock:
            info = self.clients.pop(sock, None)
        try:
            sock.close()
        except OSError:
            pass
        if info:
            self._free_colour(info.get("colour", C_WHITE))
        return info

    # ── console output ─────────────────────────────────────────────────────────
    def _print(self, sender, body, colour=C_WHITE):
        print(f"\r{C_DIM}[{now_str()}]{C_RESET} {colour}{C_BOLD}{sender}{C_RESET}: {body}")
        print("> ", end="", flush=True)

    # ── per-client handler ─────────────────────────────────────────────────────
    def handle_client(self, sock, addr):
        buf    = b""
        colour = self._alloc_colour()
        info   = {"name": None, "colour": colour}

        def bail(msg=None):
            if msg:
                send_msg(sock, type="error", text=msg)
            sock.close()

        try:
            sock.settimeout(HANDSHAKE_TIMEOUT)

            # 1. Send auth challenge
            if not send_msg(sock, type="auth_request"):
                return

            # 2. Receive passkey
            msg, buf = recv_msg(sock, buf)
            if msg is None:
                bail()
                return
            if msg.get("type") != "auth":
                bail("Unexpected handshake message.")
                return
            if hash_passkey(msg.get("passkey", "")) != self.pk_hash:
                bail("Wrong passkey.")
                self._print("System", f"Failed auth attempt from {addr[0]}", C_RED)
                return

            # 3. Auth OK
            if not send_msg(sock, type="auth_ok"):
                return

            # 4. Receive hello / name
            msg, buf = recv_msg(sock, buf)
            if msg is None or msg.get("type") != "hello":
                bail("Expected hello message.")
                return

            name = (msg.get("name") or "").strip()[:20]
            if not name:
                name = f"User_{addr[1]}"

            # Prevent duplicate names
            with self.lock:
                existing_names = {c["name"] for c in self.clients.values()}
            if name in existing_names:
                suffix = 2
                base = name[:17]
                while f"{base}_{suffix}" in existing_names:
                    suffix += 1
                name = f"{base}_{suffix}"

            info["name"] = name

            # 5. Check room capacity
            with self.lock:
                if len(self.clients) >= MAX_MEMBERS - 1:
                    bail(f"Room is full (max {MAX_MEMBERS - 1} members).")
                    return
                self.clients[sock] = info

            # 6. Send message history
            hist = self.db.get_history(self.room_id)
            send_msg(sock, type="history", messages=[
                {"sender": r[0], "body": r[1], "ts": r[2]} for r in hist
            ])

            # 7. Announce join
            notice = f"➜ {name} joined"
            self.broadcast(
                {"type": "msg", "sender": "System", "body": notice, "ts": now_str()},
                exclude=sock
            )
            with self.lock:
                online = [c["name"] for c in self.clients.values()]
            send_msg(sock, type="welcome", room_members=online)
            self.db.save_message(self.room_id, "System", notice)
            self._print("System", notice, C_DIM)

            # 8. Message loop
            sock.settimeout(None)
            while True:
                msg, buf = recv_msg(sock, buf)
                if msg is None:
                    break   # disconnected

                t = msg.get("type")

                if t == "msg":
                    body = (msg.get("body") or "").strip()
                    # Enforce max message size
                    if len(body) > 2000:
                        body = body[:2000] + "…"
                    if not body:
                        continue
                    ts = now_str()
                    pl = {"type": "msg", "sender": name, "body": body, "ts": ts}
                    self.db.save_message(self.room_id, name, body)
                    self.broadcast(pl, exclude=sock)
                    self._print(name, body, colour)

                elif t == "ping":
                    send_msg(sock, type="pong")

                # Unknown message types are silently ignored

        except socket.timeout:
            send_msg(sock, type="error", text="Handshake timed out.")
        except Exception as e:
            # Unexpected: log but don't crash server
            pass
        finally:
            gone = self._drop(sock, reason="handler_exit")
            if gone and gone.get("name"):
                leave = f"⬅ {gone['name']} left"
                self.broadcast({"type": "msg", "sender": "System", "body": leave, "ts": now_str()})
                self.db.save_message(self.room_id, "System", leave)
                self._print("System", leave, C_DIM)

    # ── host keyboard input ────────────────────────────────────────────────────
    def host_input_loop(self):
        while True:
            try:
                body = input("> ").strip()
            except (KeyboardInterrupt, EOFError):
                print(f"\n{C_DIM}Shutting down…{C_RESET}")
                self._shutdown()
                sys.exit(0)

            if not body:
                continue

            # Host commands
            if body.startswith("/"):
                self._handle_command(body)
                continue

            ts = now_str()
            self.broadcast({"type": "msg", "sender": self.host_name, "body": body, "ts": ts})
            self.db.save_message(self.room_id, self.host_name, body)
            # Reprint host's own message
            print(f"\033[A\r{C_DIM}[{ts}]{C_RESET} {C_GREEN}{C_BOLD}{self.host_name} (you){C_RESET}: {body}")
            print("> ", end="", flush=True)

    def _handle_command(self, cmd):
        parts = cmd.split(maxsplit=1)
        c = parts[0].lower()
        if c == "/who":
            with self.lock:
                names = [i["name"] for i in self.clients.values()]
            names.insert(0, f"{self.host_name} (you)")
            print(f"\r{C_DIM}Online ({len(names)}): {', '.join(names)}{C_RESET}")
        elif c == "/kick" and len(parts) > 1:
            target = parts[1].strip()
            with self.lock:
                victims = [s for s, i in self.clients.items() if i["name"] == target]
            for v in victims:
                send_msg(v, type="error", text="You have been kicked.")
                self._drop(v, reason="kicked")
            if victims:
                notice = f"⚡ {target} was kicked"
                self.broadcast({"type": "msg", "sender": "System", "body": notice, "ts": now_str()})
                self._print("System", notice, C_RED)
            else:
                print(f"\r{C_RED}User '{target}' not found.{C_RESET}")
        elif c == "/help":
            print(f"\r{C_DIM}/who          — list online members{C_RESET}")
            print(f"\r{C_DIM}/kick <name>  — remove a member{C_RESET}")
            print(f"\r{C_DIM}/quit         — shut down the room{C_RESET}")
        elif c == "/quit":
            print(f"\n{C_DIM}Shutting down…{C_RESET}")
            self._shutdown()
            sys.exit(0)
        else:
            print(f"\r{C_YEL}Unknown command. Type /help{C_RESET}")
        print("> ", end="", flush=True)

    def _shutdown(self):
        _kill_playit()
        with self.lock:
            socks = list(self.clients.keys())
        for s in socks:
            send_msg(s, type="error", text="Host has shut down the room.")
            try:
                s.close()
            except OSError:
                pass
        if self._srv_sock:
            try:
                self._srv_sock.close()
            except OSError:
                pass

    # ── main server loop ───────────────────────────────────────────────────────
    def run(self, use_tunnel=False):
        # Check port availability before binding
        if not check_port_available(HOST_PORT):
            print(f"{C_RED}Port {HOST_PORT} is already in use.{C_RESET}")
            print(f"{C_DIM}Another instance may be running, or change HOST_PORT in the script.{C_RESET}")
            sys.exit(1)

        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv_sock = srv

        try:
            srv.bind(("0.0.0.0", HOST_PORT))
        except OSError as e:
            print(f"{C_RED}Cannot bind to port {HOST_PORT}: {e}{C_RESET}")
            sys.exit(1)

        srv.listen(MAX_MEMBERS)
        lan_ip = get_lan_ip()
        pub_host, pub_port = None, None

        if use_tunnel:
            pub_host, pub_port = start_tunnel(HOST_PORT)

        # ── Print join codes ──────────────────────────────────────────────────
        lan_code = f"JOIN:{lan_ip}:{HOST_PORT}:{self.passkey}"

        print(f"\n{C_GREEN}{C_BOLD}✓ Room is open!{C_RESET}  "
              f"{C_DIM}You are: {C_BOLD}{self.host_name}{C_RESET}\n")
        print(f"  {C_BOLD}── Share one of these join codes ──{C_RESET}\n")
        print(f"  {C_DIM}Same Wi-Fi / LAN:{C_RESET}")
        print(f"    {C_YEL}{C_BOLD}{lan_code}{C_RESET}")

        if pub_host and pub_port:
            inet_code = f"JOIN:{pub_host}:{pub_port}:{self.passkey}"
            print(f"\n  {C_DIM}Internet (anywhere in the world):{C_RESET}")
            print(f"    {C_YEL}{C_BOLD}{inet_code}{C_RESET}")
        elif use_tunnel:
            print(f"\n  {C_YEL}(Internet tunnel unavailable — LAN code still works){C_RESET}")

        print(f"\n  {C_DIM}Members run  python run.py  and paste the code.{C_RESET}")
        print(f"  {C_DIM}Up to {MAX_MEMBERS - 1} members. Type /help for commands.{C_RESET}")
        print(f"\n{C_DIM}{'─' * 46}{C_RESET}\n")
        print("> ", end="", flush=True)

        # ── Accept loop in background thread ──────────────────────────────────
        def accept_loop():
            while True:
                try:
                    conn, addr = srv.accept()
                    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    threading.Thread(
                        target=self.handle_client,
                        args=(conn, addr),
                        daemon=True
                    ).start()
                except OSError:
                    break   # server socket closed

        threading.Thread(target=accept_loop, daemon=True).start()
        self.host_input_loop()

# ─── Client ────────────────────────────────────────────────────────────────────
class ChatClient:
    def __init__(self, name, host_ip, host_port, passkey):
        self.name      = name
        self.host_ip   = host_ip
        self.host_port = host_port
        self.passkey   = passkey
        self.sock      = None
        self.buf       = b""
        self.running   = False

    def _recv(self):
        msg, self.buf = recv_msg(self.sock, self.buf)
        return msg

    def _send(self, **kw):
        return send_msg(self.sock, **kw)

    def _print(self, sender, body, ts=""):
        ts_str = f"{C_DIM}[{ts}]{C_RESET} " if ts else ""
        if sender == "System":
            print(f"\r{ts_str}{C_DIM}{body}{C_RESET}")
        elif sender == self.name:
            print(f"\r{ts_str}{C_GREEN}{C_BOLD}{sender} (you){C_RESET}: {body}")
        else:
            print(f"\r{ts_str}{C_CYAN}{C_BOLD}{sender}{C_RESET}: {body}")
        print("> ", end="", flush=True)

    def receive_loop(self):
        """Background thread: receive and display incoming messages."""
        while self.running:
            try:
                msg = self._recv()
            except Exception:
                msg = None

            if msg is None:
                if self.running:
                    print(f"\n{C_RED}Disconnected from host.{C_RESET}")
                os._exit(0)

            t = msg.get("type")
            if t == "msg":
                self._print(msg.get("sender", "?"), msg.get("body", ""), msg.get("ts", ""))
            elif t == "error":
                print(f"\n{C_RED}Server: {msg.get('text', 'Unknown error')}{C_RESET}")
                os._exit(1)
            elif t == "pong":
                pass   # keepalive response, ignore
            # Other types silently ignored

    def _connect_with_retry(self, retries=3, delay=3):
        """Attempt TCP connection with retries."""
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.sock.settimeout(CONNECT_TIMEOUT)
                self.sock.connect((self.host_ip, self.host_port))
                self.sock.settimeout(None)
                return True
            except socket.timeout:
                last_error = "timeout"
                print(f"{C_YEL}Attempt {attempt}/{retries}: timed out.{C_RESET}")
            except ConnectionRefusedError:
                last_error = "refused"
                print(f"{C_YEL}Attempt {attempt}/{retries}: connection refused.{C_RESET}")
            except socket.gaierror as e:
                # DNS / hostname error — no point retrying
                print(f"{C_RED}Cannot resolve host '{self.host_ip}': {e}{C_RESET}")
                return False
            except OSError as e:
                last_error = str(e)
                print(f"{C_YEL}Attempt {attempt}/{retries}: {e}{C_RESET}")

            if attempt < retries:
                print(f"{C_DIM}Retrying in {delay}s…{C_RESET}")
                time.sleep(delay)

        # All retries failed
        if last_error == "timeout":
            print(f"{C_RED}Connection timed out after {retries} attempts.{C_RESET}")
            print(f"{C_DIM}  • Make sure the host is running{C_RESET}")
            print(f"{C_DIM}  • LAN code: both devices must be on the same network{C_RESET}")
            print(f"{C_DIM}  • Internet code: playit agent must be running on host's machine{C_RESET}")
        elif last_error == "refused":
            print(f"{C_RED}Connection refused — host may not be running yet.{C_RESET}")
        else:
            print(f"{C_RED}Could not connect: {last_error}{C_RESET}")
        return False

    def run(self):
        print(f"{C_DIM}Connecting to {self.host_ip}:{self.host_port}…{C_RESET}", flush=True)

        if not self._connect_with_retry():
            sys.exit(1)

        print(f"{C_GREEN}Connected!{C_RESET}", flush=True)

        # ── Handshake ─────────────────────────────────────────────────────────

        # Expect auth_request
        msg = self._recv()
        if msg is None:
            print(f"{C_RED}No response from host. The room may have closed.{C_RESET}")
            sys.exit(1)
        if msg.get("type") == "error":
            print(f"{C_RED}{msg.get('text', 'Rejected by server.')}{C_RESET}")
            sys.exit(1)
        if msg.get("type") != "auth_request":
            print(f"{C_RED}Unexpected server response (got '{msg.get('type')}'). Wrong port?{C_RESET}")
            sys.exit(1)

        # Send passkey
        self._send(type="auth", passkey=self.passkey)

        # Expect auth_ok or error
        msg = self._recv()
        if msg is None:
            print(f"{C_RED}No response after passkey — connection dropped.{C_RESET}")
            sys.exit(1)
        if msg.get("type") == "error":
            print(f"{C_RED}{msg.get('text', 'Wrong passkey.')}{C_RESET}")
            sys.exit(1)
        if msg.get("type") != "auth_ok":
            print(f"{C_RED}Unexpected auth response (got '{msg.get('type')}').{C_RESET}")
            sys.exit(1)

        # Send name
        self._send(type="hello", name=self.name)

        # Expect history
        msg = self._recv()
        if msg and msg.get("type") == "history":
            hist = msg.get("messages", [])
            if hist:
                print(f"\n{C_DIM}── Last {len(hist)} messages ──{C_RESET}")
                for m in hist:
                    self._print(m.get("sender", "?"), m.get("body", ""), m.get("ts", ""))
            msg = self._recv()   # proceed to next message

        # Expect welcome or error
        if msg is None:
            print(f"{C_RED}Lost connection during join.{C_RESET}")
            sys.exit(1)
        if msg.get("type") == "error":
            print(f"{C_RED}{msg.get('text', 'Rejected.')}{C_RESET}")
            sys.exit(1)
        if msg.get("type") == "welcome":
            online = msg.get("room_members", [])
            print(f"\n{C_GREEN}{C_BOLD}✓ Joined!{C_RESET}  "
                  f"{C_DIM}Online: {', '.join(online) or '—'}{C_RESET}")

        print(f"{C_DIM}Type your messages. Ctrl+C to leave.{C_RESET}\n")
        print("> ", end="", flush=True)

        # ── Message loop ──────────────────────────────────────────────────────
        self.running = True
        rx_thread = threading.Thread(target=self.receive_loop, daemon=True)
        rx_thread.start()

        while self.running:
            try:
                body = input("").strip()
            except (KeyboardInterrupt, EOFError):
                break

            if not body:
                print("> ", end="", flush=True)
                continue

            if not self._send(type="msg", body=body):
                print(f"\n{C_RED}Failed to send — connection lost.{C_RESET}")
                break

            ts = now_str()
            print(f"\033[A\r{C_DIM}[{ts}]{C_RESET} "
                  f"{C_GREEN}{C_BOLD}{self.name} (you){C_RESET}: {body}")
            print("> ", end="", flush=True)

        self.running = False
        try:
            self.sock.close()
        except OSError:
            pass
        print(f"\n{C_DIM}Disconnected. Bye!{C_RESET}")

# ─── Join code parser ──────────────────────────────────────────────────────────
def parse_join_code(raw):
    """
    Accepts:  JOIN:<host>:<port>:<passkey>
              or just <host>:<port>:<passkey>  (without prefix)
    Returns:  (host, port_int, passkey_str) or raises ValueError.
    """
    s = raw.strip()
    if not s:
        raise ValueError("Empty join code.")

    # Strip optional prefix (case-insensitive)
    if s.upper().startswith("JOIN:"):
        s = s[5:]

    # Split on ':' — but handle IPv6 addresses in brackets: [::1]:9999:PASSKEY
    ipv6_match = re.match(r"^(\[.+\]):(\d+):([A-Za-z0-9]+)$", s)
    if ipv6_match:
        host    = ipv6_match.group(1)
        port_s  = ipv6_match.group(2)
        passkey = ipv6_match.group(3)
    else:
        parts = s.split(":")
        if len(parts) < 3:
            raise ValueError(
                "Expected format: JOIN:<host>:<port>:<passkey>\n"
                "  e.g. JOIN:192.168.1.5:9999:AB3X7Q\n"
                "  e.g. JOIN:abc.at.ply.gg:47234:AB3X7Q"
            )
        # Last part is passkey, second-to-last is port, everything before is host
        passkey = parts[-1]
        port_s  = parts[-2]
        host    = ":".join(parts[:-2])

    host = host.strip()
    if not host:
        raise ValueError("Host/IP cannot be empty.")

    try:
        port = int(port_s)
    except ValueError:
        raise ValueError(f"Port must be a number, got: {port_s!r}")

    if not (1 <= port <= 65535):
        raise ValueError(f"Port {port} is out of valid range (1–65535).")

    passkey = passkey.strip().upper()
    if not passkey:
        raise ValueError("Passkey cannot be empty.")
    if not re.match(r"^[A-Z0-9]+$", passkey):
        raise ValueError(f"Passkey contains invalid characters: {passkey!r}")

    return host, port, passkey

# ─── Startup checks ────────────────────────────────────────────────────────────
def check_python_version():
    if sys.version_info < (3, 7):
        print(f"{C_RED}Python 3.7 or newer is required "
              f"(you have {sys.version}).{C_RESET}")
        sys.exit(1)

def check_dependencies():
    """Warn about anything that might cause issues."""
    # sqlite3 is stdlib but can be missing in minimal builds
    try:
        import sqlite3
    except ImportError:
        print(f"{C_RED}sqlite3 module not found. "
              f"Install python3-sqlite or use a full Python build.{C_RESET}")
        sys.exit(1)

# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    check_python_version()
    check_dependencies()
    banner()

    # Graceful Ctrl+C at top level
    signal.signal(signal.SIGINT, lambda *_: (print(f"\n{C_DIM}Bye!{C_RESET}"), sys.exit(0)))

    print(f"  {C_BOLD}1{C_RESET} → {C_MAG}Host{C_RESET}   — create a room on this machine")
    print(f"  {C_BOLD}2{C_RESET} → {C_CYAN}Member{C_RESET} — join with a code from the host\n")
    choice = choice_prompt("Enter 1 or 2: ", ("1", "2"))

    # ── HOST ──────────────────────────────────────────────────────────────────
    if choice == "1":
        print(f"\n{C_MAG}{C_BOLD}── Host setup ──{C_RESET}")

        host_name = ""
        while not host_name:
            host_name = input_safe("Your name: ")[:20]
            if not host_name:
                print(f"{C_RED}Name cannot be empty.{C_RESET}")

        print(f"\n{C_DIM}Do members need to join from outside your Wi-Fi / LAN?{C_RESET}")
        print(f"  {C_BOLD}1{C_RESET} → No  — same Wi-Fi / LAN only")
        print(f"  {C_BOLD}2{C_RESET} → Yes — internet via playit.gg (free, no credit card)\n")
        nc = choice_prompt("Enter 1 or 2: ", ("1", "2"))

        passkey = gen_passkey()
        db      = Database(DB_FILE)
        server  = ChatServer(host_name, passkey, db)
        server.run(use_tunnel=(nc == "2"))

    # ── MEMBER ────────────────────────────────────────────────────────────────
    else:
        print(f"\n{C_CYAN}{C_BOLD}── Member setup ──{C_RESET}")
        print(f"{C_DIM}The host will share a join code with you. Examples:{C_RESET}")
        print(f"  {C_DIM}JOIN:192.168.1.10:9999:AB3X7Q         ← LAN{C_RESET}")
        print(f"  {C_DIM}JOIN:abc.at.ply.gg:47234:AB3X7Q       ← internet{C_RESET}\n")

        host_ip = host_port = passkey = None
        for attempt in range(1, 6):
            raw = input_safe("Paste join code: ")
            try:
                host_ip, host_port, passkey = parse_join_code(raw)
                break
            except ValueError as e:
                remaining = 5 - attempt
                print(f"{C_RED}  ✗ {e}{C_RESET}")
                if remaining == 0:
                    print(f"{C_RED}Too many invalid attempts. Exiting.{C_RESET}")
                    sys.exit(1)
                print(f"{C_DIM}  ({remaining} tries left){C_RESET}")
        else:
            sys.exit(1)

        name = ""
        while not name:
            name = input_safe("Your name: ")[:20]
            if not name:
                print(f"{C_RED}Name cannot be empty.{C_RESET}")

        ChatClient(name, host_ip, host_port, passkey).run()


if __name__ == "__main__":
    main()
