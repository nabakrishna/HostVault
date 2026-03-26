#!/usr/bin/env python3
"""
LocalChat — LAN chat room, no dependencies beyond Python stdlib.
Usage: python run.py
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
import string
import queue
from datetime import datetime

# ─── Config ────────────────────────────────────────────────────────────────────
HOST_PORT        = 9999
MAX_MEMBERS      = 5       # host + 4 others
BUFFER           = 4096
DB_FILE          = "chat.db"
CONNECT_TIMEOUT  = 10      # seconds to wait while TCP connecting
HANDSHAKE_TIMEOUT = 120    # seconds for the full auth+name exchange (generous)

# ─── ANSI colours (degrade gracefully on Windows) ──────────────────────────────
def _ansi(code): return f"\033[{code}m" if sys.platform != "win32" or os.environ.get("TERM") else ""

C_RESET   = _ansi("0")
C_BOLD    = _ansi("1")
C_DIM     = _ansi("2")
C_CYAN    = _ansi("96")
C_YELLOW  = _ansi("93")
C_GREEN   = _ansi("92")
C_RED     = _ansi("91")
C_MAGENTA = _ansi("95")
C_BLUE    = _ansi("94")
C_WHITE   = _ansi("97")

MEMBER_COLOURS = [C_CYAN, C_YELLOW, C_GREEN, C_MAGENTA, C_BLUE]

def banner():
    print(f"""
{C_BOLD}{C_CYAN}╔══════════════════════════════════════╗
║         LocalChat  v1.0              ║
║   LAN chat — no internet needed      ║
╚══════════════════════════════════════╝{C_RESET}
""")

def clear_line():
    print("\r" + " " * 60 + "\r", end="", flush=True)

# ─── Utilities ─────────────────────────────────────────────────────────────────
def gen_passkey(length=6):
    """Generate a short, readable passkey (no ambiguous chars)."""
    chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(random.choices(chars, k=length))

def hash_passkey(pk):
    return hashlib.sha256(pk.encode()).hexdigest()

def now_str():
    return datetime.now().strftime("%H:%M:%S")

def input_safe(prompt):
    """input() with KeyboardInterrupt → clean exit."""
    try:
        return input(prompt).strip()
    except (KeyboardInterrupt, EOFError):
        print(f"\n{C_DIM}Bye!{C_RESET}")
        sys.exit(0)

# ─── Database (host only) ───────────────────────────────────────────────────────
class Database:
    def __init__(self, path):
        self.path = path
        self._local = threading.local()
        self._init_schema()

    def _conn(self):
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self.path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return self._local.conn

    def _init_schema(self):
        with sqlite3.connect(self.path) as c:
            c.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS rooms (
                    id        INTEGER PRIMARY KEY,
                    passkey   TEXT UNIQUE NOT NULL,
                    host_name TEXT NOT NULL,
                    created   TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id        INTEGER PRIMARY KEY,
                    room_id   INTEGER NOT NULL,
                    sender    TEXT NOT NULL,
                    body      TEXT NOT NULL,
                    ts        TEXT NOT NULL,
                    FOREIGN KEY(room_id) REFERENCES rooms(id)
                );
                CREATE INDEX IF NOT EXISTS idx_messages_room ON messages(room_id);
            """)

    def create_room(self, passkey_hash, host_name):
        c = self._conn()
        cur = c.execute(
            "INSERT INTO rooms (passkey, host_name, created) VALUES (?,?,?)",
            (passkey_hash, host_name, datetime.now().isoformat())
        )
        c.commit()
        return cur.lastrowid

    def verify_passkey(self, passkey_hash):
        row = self._conn().execute(
            "SELECT id FROM rooms WHERE passkey=?", (passkey_hash,)
        ).fetchone()
        return row[0] if row else None

    def save_message(self, room_id, sender, body):
        c = self._conn()
        c.execute(
            "INSERT INTO messages (room_id, sender, body, ts) VALUES (?,?,?,?)",
            (room_id, sender, body, datetime.now().isoformat())
        )
        c.commit()

    def get_history(self, room_id, limit=50):
        rows = self._conn().execute(
            "SELECT sender, body, ts FROM messages WHERE room_id=? "
            "ORDER BY id DESC LIMIT ?", (room_id, limit)
        ).fetchall()
        return list(reversed(rows))

# ─── Protocol helpers ───────────────────────────────────────────────────────────
# Messages are newline-delimited JSON.
def send_msg(sock, **kwargs):
    try:
        data = json.dumps(kwargs) + "\n"
        sock.sendall(data.encode())
    except OSError:
        pass

def recv_msg(sock, buf=b""):
    """Read one newline-delimited JSON frame. Returns (parsed_dict, leftover_bytes)."""
    while b"\n" not in buf:
        try:
            chunk = sock.recv(BUFFER)
            if not chunk:
                return None, buf
            buf += chunk
        except OSError:
            return None, buf
    line, remainder = buf.split(b"\n", 1)
    try:
        return json.loads(line.decode()), remainder
    except json.JSONDecodeError:
        return None, remainder

# ─── HOST MODE ──────────────────────────────────────────────────────────────────
class ChatServer:
    def __init__(self, host_name, passkey, db):
        self.host_name  = host_name
        self.passkey    = passkey
        self.pk_hash    = hash_passkey(passkey)
        self.db         = db
        self.room_id    = db.create_room(self.pk_hash, host_name)
        self.clients    = {}       # sock -> {"name": str, "colour": str, "buf": bytes}
        self.lock       = threading.Lock()
        self.colour_pool = list(MEMBER_COLOURS)
        random.shuffle(self.colour_pool)

    def _colour_for(self):
        if self.colour_pool:
            return self.colour_pool.pop(0)
        return C_WHITE

    def broadcast(self, payload, exclude=None):
        with self.lock:
            dead = []
            for s, info in self.clients.items():
                if s is exclude:
                    continue
                try:
                    s.sendall((json.dumps(payload) + "\n").encode())
                except OSError:
                    dead.append(s)
            for s in dead:
                self._remove_client(s)

    def _remove_client(self, sock):
        info = self.clients.pop(sock, None)
        try:
            sock.close()
        except OSError:
            pass
        return info

    def _print_chat(self, sender, body, colour=C_WHITE):
        ts = now_str()
        print(f"\r{C_DIM}[{ts}]{C_RESET} {colour}{C_BOLD}{sender}{C_RESET}: {body}")
        print("> ", end="", flush=True)

    def handle_client(self, sock, addr):
        buf = b""
        info = {"name": None, "colour": self._colour_for(), "buf": b""}

        try:
            # Give the member plenty of time to complete the handshake
            # (they may be slow typing their name)
            sock.settimeout(HANDSHAKE_TIMEOUT)

            # ── Step 1: passkey auth ──
            send_msg(sock, type="auth_request")
            msg, buf = recv_msg(sock, buf)
            if not msg or msg.get("type") != "auth" or hash_passkey(msg.get("passkey","")) != self.pk_hash:
                send_msg(sock, type="error", text="Wrong passkey. Connection refused.")
                sock.close()
                return

            # ── Step 2: name exchange ──
            send_msg(sock, type="auth_ok")
            msg, buf = recv_msg(sock, buf)
            if not msg or msg.get("type") != "hello":
                sock.close()
                return

            name = (msg.get("name") or "").strip()[:20] or f"User_{addr[1]}"
            info["name"] = name

            with self.lock:
                if len(self.clients) >= MAX_MEMBERS - 1:
                    send_msg(sock, type="error", text="Room is full (max 5 members).")
                    sock.close()
                    return
                self.clients[sock] = info

            # ── Step 3: send history ──
            history = self.db.get_history(self.room_id)
            send_msg(sock, type="history", messages=[
                {"sender": r[0], "body": r[1], "ts": r[2]} for r in history
            ])

            # ── Step 4: announce join ──
            join_notice = f"➜ {name} joined the room"
            self.broadcast({"type": "msg", "sender": "System", "body": join_notice, "ts": now_str()}, exclude=sock)
            send_msg(sock, type="welcome", room_members=[c["name"] for c in self.clients.values()])
            self.db.save_message(self.room_id, "System", join_notice)
            self._print_chat("System", join_notice, C_DIM)

            # ── Step 5: message loop — no timeout, stay open until disconnect ──
            sock.settimeout(None)
            while True:
                msg, buf = recv_msg(sock, buf)
                if msg is None:
                    break
                if msg.get("type") == "msg":
                    body = (msg.get("body") or "").strip()
                    if not body:
                        continue
                    ts = now_str()
                    payload = {"type": "msg", "sender": name, "body": body, "ts": ts}
                    self.db.save_message(self.room_id, name, body)
                    self.broadcast(payload, exclude=sock)
                    self._print_chat(name, body, info["colour"])

        except Exception as e:
            pass
        finally:
            with self.lock:
                gone = self._remove_client(sock)
            if gone and gone.get("name"):
                leave = f"⬅ {gone['name']} left the room"
                self.broadcast({"type": "msg", "sender": "System", "body": leave, "ts": now_str()})
                self.db.save_message(self.room_id, "System", leave)
                self._print_chat("System", leave, C_DIM)
            if gone:
                self.colour_pool.append(gone.get("colour", C_WHITE))

    def host_input_loop(self):
        """Let the host type messages from the terminal."""
        while True:
            try:
                body = input("> ").strip()
            except (KeyboardInterrupt, EOFError):
                print(f"\n{C_DIM}Shutting down…{C_RESET}")
                os._exit(0)
            if not body:
                continue
            ts = now_str()
            payload = {"type": "msg", "sender": self.host_name, "body": body, "ts": ts}
            self.broadcast(payload)
            self.db.save_message(self.room_id, self.host_name, body)
            # print own message
            print(f"\033[A\r{C_DIM}[{ts}]{C_RESET} {C_GREEN}{C_BOLD}{self.host_name} (you){C_RESET}: {body}")
            print("> ", end="", flush=True)

    def run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            srv.bind(("0.0.0.0", HOST_PORT))
        except OSError as e:
            print(f"{C_RED}Cannot bind port {HOST_PORT}: {e}{C_RESET}")
            sys.exit(1)
        srv.listen(MAX_MEMBERS)

        # Get LAN IP
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            lan_ip = s.getsockname()[0]
            s.close()
        except Exception:
            lan_ip = "your-ip"

        print(f"{C_GREEN}{C_BOLD}✓ Room created!{C_RESET}")
        print(f"  {C_BOLD}Passkey:{C_RESET}  {C_YELLOW}{C_BOLD}{self.passkey}{C_RESET}")
        print(f"  {C_BOLD}Share with members:{C_RESET}  {C_CYAN}IP = {lan_ip}  Port = {HOST_PORT}{C_RESET}")
        print(f"  {C_DIM}Up to {MAX_MEMBERS-1} members can join.{C_RESET}\n")
        print(f"{C_DIM}Type your messages below. Ctrl+C to quit.{C_RESET}\n")
        print("> ", end="", flush=True)

        # Accept thread
        def accept_loop():
            while True:
                try:
                    conn, addr = srv.accept()
                    t = threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True)
                    t.start()
                except OSError:
                    break

        threading.Thread(target=accept_loop, daemon=True).start()
        self.host_input_loop()

# ─── MEMBER MODE ─────────────────────────────────────────────────────────────
class ChatClient:
    def __init__(self, name, host_ip, passkey):
        self.name    = name
        self.host_ip = host_ip
        self.passkey = passkey
        self.sock    = None
        self.buf     = b""
        self.running = False

    def _recv(self):
        msg, self.buf = recv_msg(self.sock, self.buf)
        return msg

    def _send(self, **kwargs):
        send_msg(self.sock, **kwargs)

    def _print_msg(self, sender, body, ts=""):
        ts_str = f"{C_DIM}[{ts}]{C_RESET} " if ts else ""
        if sender == "System":
            print(f"\r{ts_str}{C_DIM}{body}{C_RESET}")
        elif sender == self.name:
            print(f"\r{ts_str}{C_GREEN}{C_BOLD}{sender} (you){C_RESET}: {body}")
        else:
            print(f"\r{ts_str}{C_CYAN}{C_BOLD}{sender}{C_RESET}: {body}")
        print("> ", end="", flush=True)

    def receive_loop(self):
        while self.running:
            try:
                msg = self._recv()
                if msg is None:
                    print(f"\n{C_RED}Disconnected from host.{C_RESET}")
                    self.running = False
                    os._exit(0)
                    break
                mtype = msg.get("type")
                if mtype == "msg":
                    self._print_msg(msg.get("sender","?"), msg.get("body",""), msg.get("ts",""))
                elif mtype == "error":
                    print(f"\n{C_RED}Error: {msg.get('text')}{C_RESET}")
                    self.running = False
                    break
            except Exception:
                if self.running:
                    print(f"\n{C_RED}Connection lost.{C_RESET}")
                self.running = False
                break

    def run(self):
        # ── Connect (short timeout only for the TCP handshake itself) ──
        print(f"{C_DIM}Connecting to {self.host_ip}:{HOST_PORT}…{C_RESET}", flush=True)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(CONNECT_TIMEOUT)
        try:
            self.sock.connect((self.host_ip, HOST_PORT))
        except socket.timeout:
            print(f"{C_RED}Connection timed out. Check the IP address and make sure both devices are on the same network.{C_RESET}")
            sys.exit(1)
        except ConnectionRefusedError:
            print(f"{C_RED}Connection refused. Make sure the host has started the room on port {HOST_PORT}.{C_RESET}")
            sys.exit(1)
        except OSError as e:
            print(f"{C_RED}Cannot connect: {e}{C_RESET}")
            sys.exit(1)

        # TCP connect succeeded — remove timeout so recv never times out during chat
        self.sock.settimeout(None)
        print(f"{C_GREEN}Connected!{C_RESET}", flush=True)

        # ── Auth ──
        msg = self._recv()
        if not msg:
            print(f"{C_RED}No response from host. The host may have closed the room.{C_RESET}")
            sys.exit(1)
        if msg.get("type") != "auth_request":
            print(f"{C_RED}Unexpected response from host (type={msg.get('type')}). Are you connecting to the right port?{C_RESET}")
            sys.exit(1)

        self._send(type="auth", passkey=self.passkey)
        msg = self._recv()
        if not msg:
            print(f"{C_RED}No response after sending passkey. Connection dropped.{C_RESET}")
            sys.exit(1)
        if msg.get("type") == "error":
            print(f"{C_RED}{msg.get('text', 'Authentication failed.')}{C_RESET}")
            sys.exit(1)
        if msg.get("type") != "auth_ok":
            print(f"{C_RED}Unexpected response after passkey (type={msg.get('type')}).{C_RESET}")
            sys.exit(1)

        # ── Send name ──
        self._send(type="hello", name=self.name)

        # ── Receive history + welcome ──
        msg = self._recv()
        if msg and msg.get("type") == "history":
            history = msg.get("messages", [])
            if history:
                print(f"\n{C_DIM}── Last {len(history)} messages ──{C_RESET}")
                for m in history:
                    self._print_msg(m["sender"], m["body"], m.get("ts",""))
            msg = self._recv()   # expect welcome

        if msg and msg.get("type") == "error":
            print(f"{C_RED}{msg.get('text', 'Rejected by host.')}{C_RESET}")
            sys.exit(1)

        if msg and msg.get("type") == "welcome":
            members = msg.get("room_members", [])
            print(f"\n{C_GREEN}{C_BOLD}✓ Joined the room!{C_RESET}  "
                  f"{C_DIM}Members online: {', '.join(members)}{C_RESET}")

        print(f"{C_DIM}Type your messages. Ctrl+C to leave.\n{C_RESET}")
        print("> ", end="", flush=True)

        # ── Start receive thread ──
        self.running = True
        t = threading.Thread(target=self.receive_loop, daemon=True)
        t.start()

        # ── Send loop ──
        while self.running:
            try:
                body = input("").strip()
            except (KeyboardInterrupt, EOFError):
                break
            if not body:
                print("> ", end="", flush=True)
                continue
            self._send(type="msg", body=body)
            # Print own message (server won't echo back)
            ts = now_str()
            print(f"\033[A\r{C_DIM}[{ts}]{C_RESET} {C_GREEN}{C_BOLD}{self.name} (you){C_RESET}: {body}")
            print("> ", end="", flush=True)

        self.running = False
        try:
            self.sock.close()
        except OSError:
            pass
        print(f"\n{C_DIM}Disconnected. Bye!{C_RESET}")

# ─── Entry point ───────────────────────────────────────────────────────────────
def main():
    banner()

    print("How do you want to join?")
    print(f"  {C_BOLD}1{C_RESET} → {C_MAGENTA}Host{C_RESET}   (start the chat room on this laptop)")
    print(f"  {C_BOLD}2{C_RESET} → {C_CYAN}Member{C_RESET} (join an existing room)\n")

    choice = input_safe("Enter 1 or 2: ")
    while choice not in ("1", "2"):
        choice = input_safe("Please enter 1 (Host) or 2 (Member): ")

    if choice == "1":
        # ── HOST setup ──
        print(f"\n{C_MAGENTA}{C_BOLD}── Host setup ──{C_RESET}")
        host_name = input_safe("Your name: ")
        while not host_name:
            host_name = input_safe("Name cannot be empty: ")
        host_name = host_name[:20]

        passkey = gen_passkey()
        print(f"\n{C_BOLD}Your room passkey:{C_RESET}  {C_YELLOW}{C_BOLD}{passkey}{C_RESET}")
        print(f"{C_DIM}Share this with people who want to join.\n{C_RESET}")

        db = Database(DB_FILE)
        server = ChatServer(host_name, passkey, db)
        server.run()

    else:
        # ── MEMBER setup ──
        print(f"\n{C_CYAN}{C_BOLD}── Member setup ──{C_RESET}")
        host_ip = input_safe("Host's IP address: ")
        while not host_ip:
            host_ip = input_safe("IP cannot be empty: ")

        passkey = input_safe("Passkey (from host): ").upper().strip()
        while not passkey:
            passkey = input_safe("Passkey cannot be empty: ").upper().strip()

        name = input_safe("Your name: ")
        while not name:
            name = input_safe("Name cannot be empty: ")
        name = name[:20]

        client = ChatClient(name, host_ip, passkey)
        client.run()

if __name__ == "__main__":
    main()
