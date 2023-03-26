"""Util file."""


def get_message_str(socket, clientsocket):
    """Fix style issue."""
    with clientsocket:
        message_chunks = []
        while True:
            try:
                data = clientsocket.recv(4096)
            except socket.timeout:
                continue
            if not data:
                break
            message_chunks.append(data)
    # Decode list-of-byte-strings to UTF8 and parse JSON data
    message_bytes = b''.join(message_chunks)
    message_str = message_bytes.decode("utf-8")
    return message_str


def create_socket(host, port, sock):
    """Fix style issue."""
    sock.bind((host, port))
    sock.listen()
    sock.settimeout(1)
    print("Created server socket")
