import socket  # noqa: F401
import struct


def parse_kafka_header(data):
    """
    Parse the Kafka request header and extract the fields.

    Parameters:
    - data (bytes): The raw bytes received from the socket.

    Returns:
    - dict: A dictionary containing the parsed fields from the header.
    """
    # Kafka Request Header structure:
    # message_size (4 bytes INT32)
    # request_api_key (2 bytes INT16)
    # request_api_version (2 bytes INT16)
    # correlation_id (4 bytes INT32)
    # client_id (nullable string with 2-byte length prefix)
    # tag_buffer (optional compact array, skipped in this example)

    if len(data) < 10:
        raise ValueError("Incomplete Kafka header")

    # Unpack fixed-length fields
    message_size, request_api_key, request_api_version, correlation_id = struct.unpack_from('>ihhI', data, 0)

    # Read the client_id
    client_id_length = struct.unpack_from('>h', data, 10)[0]
    offset = 12
    if client_id_length == -1:  # NULLABLE_STRING
        client_id = None
    else:
        client_id = data[offset:offset + client_id_length].decode('utf-8')
        offset += client_id_length

    return {
        'message_size': message_size,
        'request_api_key': request_api_key,
        'request_api_version': request_api_version,
        'correlation_id': correlation_id,
        'client_id': client_id
    }


def create_message(id):
    id_bytes = id.to_bytes(4, "big")
    return len(id_bytes).to_bytes(4, "big") + id_bytes

def handle_client(client):
    data = client.recv(1024)
    print(f"Received data: {data}")
    kafka_info_dict = parse_kafka_header(data)
    correlation_id = kafka_info_dict["correlation_id"]
    request_api_version = kafka_info_dict["request_api_version"]
    print(f"Request API version: {request_api_version}")
    UNSUPPORTED_API_VERSION = 35
    SUCCESS = 0
    if not 0 <= request_api_version <= 4:
        client.sendall(create_message(correlation_id) + UNSUPPORTED_API_VERSION.to_bytes(2, "big")) # Unsupported version
    else:
        client.sendall(create_message(correlation_id) + SUCCESS.to_bytes(2, "big"))
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
