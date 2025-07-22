import threading
import time
import socket
from datetime import datetime, timedelta, timezone

class MiniRedis:
    def __init__(self, aof_file='dump.aof'):
        self.store = {}
        self.aof_file = aof_file
        self.lock = threading.Lock()
        self.subscribers = []

        self.expirations = {}  # key: expiration datetime

        self.load_aof()

        # Start expiration cleanup thread
        self.expire_thread = threading.Thread(target=self._expire_keys_loop, daemon=True)
        self.expire_thread.start()

    def load_aof(self):
        try:
            with open(self.aof_file, 'r') as f:
                for line in f:
                    self.execute_command(line.strip(), write_aof=False)
        except FileNotFoundError:
            pass

    def write_aof(self, command):
        with open(self.aof_file, 'a') as f:
            f.write(command + '\n')

    def _expire_keys_loop(self):
        while True:
            with self.lock:
                now = datetime.now(timezone.utc)
                to_delete = [key for key, expire_time in self.expirations.items() if expire_time <= now]
                for key in to_delete:
                    self.store.pop(key, None)
                    self.expirations.pop(key, None)
                    # Notify subscribers about deletion
                    self.notify_subscribers(f"DEL {key}")
            time.sleep(1)

    def execute_command(self, command_line, write_aof=True):
        parts = command_line.strip().split()
        if not parts:
            return None

        cmd = parts[0].upper()
        args = parts[1:]

        with self.lock:
            if cmd == 'SET' and len(args) == 2:
                key, value = args
                self.store[key] = value
                self.expirations.pop(key, None)  # Remove expiration if any
            elif cmd == 'GET' and len(args) == 1:
                key = args[0]
                if key in self.expirations and self.expirations[key] <= datetime.now(timezone.utc):
                    # expired
                    self.store.pop(key, None)
                    self.expirations.pop(key, None)
                    return None
                return self.store.get(key, None)
            elif cmd == 'DEL' and len(args) == 1:
                key = args[0]
                existed = key in self.store
                self.store.pop(key, None)
                self.expirations.pop(key, None)
                if existed:
                    if write_aof:
                        self.write_aof(command_line)
                        self.notify_subscribers(command_line)
                    return 1
                else:
                    return 0
            elif cmd == 'EXPIRE' and len(args) == 2:
                key = args[0]
                seconds = int(args[1])
                if key not in self.store:
                    return 0
                expire_time = datetime.now(timezone.utc) + timedelta(seconds=seconds)
                self.expirations[key] = expire_time
                if write_aof:
                    self.write_aof(command_line)
                    self.notify_subscribers(command_line)
                return 1
            elif cmd == 'TTL' and len(args) == 1:
                key = args[0]
                if key not in self.store:
                    return -2  # key doesn't exist
                if key not in self.expirations:
                    return -1  # no expiration
                remaining = (self.expirations[key] - datetime.now(timezone.utc)).total_seconds()
                return int(remaining) if remaining > 0 else -2
            elif cmd == 'LPUSH' and len(args) >= 2:
                key = args[0]
                values = args[1:]
                if key not in self.store or not isinstance(self.store[key], list):
                    self.store[key] = []
                self.store[key] = list(values) + self.store[key]
            elif cmd == 'LRANGE' and len(args) == 3:
                key = args[0]
                start = int(args[1])
                stop = int(args[2])
                if key not in self.store or not isinstance(self.store[key], list):
                    return []
                return self.store[key][start:stop+1]
            elif cmd == 'MSET' and len(args) >= 2 and len(args) % 2 == 0:
                for i in range(0, len(args), 2):
                    key = args[i]
                    value = args[i+1]
                    self.store[key] = value
                    self.expirations.pop(key, None)  # Remove expiration if any
                if write_aof:
                    self.write_aof(command_line)
                    self.notify_subscribers(command_line)
                return "OK"
            
            elif cmd == 'MGET' and len(args) >= 1:
                result = []
                for key in args:
                    if key in self.expirations and self.expirations[key] <= datetime.now(timezone.utc):
                        # expired
                        self.store.pop(key, None)
                        self.expirations.pop(key, None)
                        result.append(None)
                    else:
                        result.append(self.store.get(key, None))
                return result
            else:
                return f"Unknown command or wrong args: {command_line}"

            if write_aof and cmd not in ('GET', 'LRANGE', 'TTL'):
                self.write_aof(command_line)
                self.notify_subscribers(command_line)

    def subscribe(self, callback):
        """Register a callback to be called with each new command line"""
        self.subscribers.append(callback)

    def notify_subscribers(self, command_line):
        for cb in self.subscribers:
            try:
                cb(command_line)
            except Exception:
                pass


class MiniRedisServer:
    def __init__(self, host='127.0.0.1', port=6379):
        self.db = MiniRedis()
        self.host = host
        self.port = port
        self.server_socket = None
        self.running = False
        self.subscriptions = {}  # channel -> set of client sockets
        self.sub_lock = threading.Lock()

    def start(self):
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()
        print(f"MiniRedis server listening on {self.host}:{self.port}")

        while self.running:
            client_sock, addr = self.server_socket.accept()
            print(f"Client connected from {addr}")
            client_thread = threading.Thread(target=self.handle_client, args=(client_sock,), daemon=True)
            client_thread.start()

    def handle_client(self, client_sock):
        with client_sock:
            client_sock.sendall(b"MiniRedis Server Ready\n")
            buffer = b''
            subscribed_channels = set()
            while True:
                data = client_sock.recv(1024)
                if not data:
                    break
                buffer += data
                while b'\n' in buffer:
                    line, buffer = buffer.split(b'\n', 1)
                    command_line = line.decode().strip()
                    if not command_line:
                        continue

                    parts = command_line.split()
                    cmd = parts[0].upper()

                    # Handle Pub/Sub commands first
                    if cmd == 'SUBSCRIBE' and len(parts) == 2:
                        channel = parts[1]
                        with self.sub_lock:
                            self.subscriptions.setdefault(channel, set()).add(client_sock)
                        subscribed_channels.add(channel)
                        client_sock.sendall(f"Subscribed to {channel}\n".encode())
                        continue

                    elif cmd == 'UNSUBSCRIBE' and len(parts) == 2:
                        channel = parts[1]
                        with self.sub_lock:
                            subs = self.subscriptions.get(channel, set())
                            subs.discard(client_sock)
                            if not subs:
                                self.subscriptions.pop(channel, None)
                        subscribed_channels.discard(channel)
                        client_sock.sendall(f"Unsubscribed from {channel}\n".encode())
                        continue

                    elif cmd == 'PUBLISH' and len(parts) >= 3:
                        channel = parts[1]
                        message = ' '.join(parts[2:])
                        self.publish(channel, message)
                        client_sock.sendall(f"Message published to {channel}\n".encode())
                        continue

                    # Else, normal MiniRedis commands
                    result = self.db.execute_command(command_line)
                    if result is None:
                        result_str = "nil\n"
                    elif isinstance(result, list):
                        result_str = "\n".join(str(item) for item in result) + "\n"
                    else:
                        result_str = str(result) + "\n"
                    client_sock.sendall(result_str.encode())

            # Clean up subscriptions on disconnect
            with self.sub_lock:
                for channel in subscribed_channels:
                    subs = self.subscriptions.get(channel, set())
                    subs.discard(client_sock)
                    if not subs:
                        self.subscriptions.pop(channel, None)
            print("Client disconnected")

    def publish(self, channel, message):
        with self.sub_lock:
            subscribers = self.subscriptions.get(channel, set()).copy()
        for client_sock in subscribers:
            try:
                client_sock.sendall(f"PUB {channel}: {message}\n".encode())
            except Exception:
                # If sending fails, remove subscriber
                with self.sub_lock:
                    self.subscriptions[channel].discard(client_sock)

    def stop(self):
        self.running = False
        if self.server_socket:
            self.server_socket.close()


if __name__ == "__main__":
    server = MiniRedisServer()
    try:
        server.start()
    except KeyboardInterrupt:
        print("Shutting down server...")
        server.stop()
