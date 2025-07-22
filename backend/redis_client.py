import socket

class MiniRedisClient:
    def __init__(self, host='localhost', port=6379):
        self.host = host
        self.port = port
        self.sock = None
        self.buffer = b""

    def connect(self):
        if self.sock:
            return
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))

    def send_command(self, command):
        self.connect()
        self.sock.sendall((command + '\n').encode())
        return self._read_response()

    def _read_response(self):
        chunks = []
        self.sock.settimeout(0.2)  # short timeout to gather all data
        try:
            while True:
                data = self.sock.recv(4096)
                if not data:
                    break
                chunks.append(data)
        except socket.timeout:
            pass
        self.sock.settimeout(None)
        response = b''.join(chunks).decode().strip()
        print(f"[redis_client] Response:\n{response}")  # Debug print
        return response

    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None
