import socket  # noqa: F401


def create_message(id):
    id_bytes = id.to_bytes(4, "big")
    return len(id_bytes).to_bytes(4, "big") + id_bytes

def handle_client(client):
    data = client.recv(1024)
    print(f"Received data: {data}")
    client.sendall(create_message(7))
    client.close()

def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    server.listen()

    while True:
        client, client_address = server.accept()
        print(f"Received connection from {client_address}")
        handle_client(client)


if __name__ == "__main__":
    main()
