import socket  # noqa: F401


def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    server.listen()

    while True:
        client, addr = server.accept()
        print(f"Connection from {addr}")
        client.close()


if __name__ == "__main__":
    main()
