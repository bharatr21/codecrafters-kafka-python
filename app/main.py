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


def create_message(response):
    response_length = len(response)
    message = response_length.to_bytes(4) + response
    return message

def make_response(kafka_info_dict):
    correlation_id = kafka_info_dict["correlation_id"]
    request_api_version = kafka_info_dict["request_api_version"]
    request_api_key = kafka_info_dict["request_api_key"]
    client_id = kafka_info_dict["client_id"]
    valid_api_versions = list(range(5))
    throttle_time_ms = 0
    UNSUPPORTED_API_VERSION = 35
    NO_ERROR = 0
    error_code = NO_ERROR if request_api_version in valid_api_versions else UNSUPPORTED_API_VERSION
    min_version, max_version = 0, 4
    tag_buffer = b"\x00" # Setting to 0 for now
    response_header = correlation_id.to_bytes(4)
    response_body = (
    error_code.to_bytes(2, "big")
    + int(1 + 1).to_bytes(1, "big") # num_api_keys = 1
    + request_api_key.to_bytes(2, "big")
    + min_version.to_bytes(2, "big")
    + max_version.to_bytes(2, "big")
    + tag_buffer # No tag buffer for API keys
    + throttle_time_ms.to_bytes(4, "big")
    + tag_buffer # No tagged fields for entire response
    )

    response = response_header + response_body
    return response

def handle_client(client):
    data = client.recv(1024)
    print(f"Received data: {data}")
    kafka_info_dict = parse_kafka_header(data)
    response = make_response(kafka_info_dict)
    message = create_message(response)
    client.sendall(message)

def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    server.listen()
    client, client_address = server.accept()
    while True:
        print(f"Received connection from {client_address}")
        handle_client(client)


if __name__ == "__main__":
    main()
