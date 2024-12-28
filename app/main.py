import socket  # noqa: F401
import struct
import threading
from enum import Enum

class ErrorCodes(Enum):
    NO_ERROR = 0
    UNKNOWN_SERVER_ERROR = 1
    COORDINATOR_NOT_AVAILABLE = 15
    UNSUPPORTED_API_VERSION = 35

class RequestTypes(Enum):
    APIVersions = {
        "value": 18,
        "MinVersion": 0,
        "MaxVersion": 4
    }
    DescribeTopicPartitions = {
        "value": 75,
        "MinVersion": 0,
        "MaxVersion": 0
    }


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


def get_request_type(request_api_key):
    for req_type in RequestTypes:
        if req_type.value["value"] == request_api_key:
            return req_type
    return None

def create_message(response):
    response_length = len(response)
    message = response_length.to_bytes(4) + response
    return message

def make_response(kafka_info_dict):
    correlation_id = kafka_info_dict["correlation_id"]
    request_api_version = kafka_info_dict["request_api_version"]
    request_api_key = kafka_info_dict["request_api_key"]
    client_id = kafka_info_dict["client_id"]
    request_type = get_request_type(request_api_key)
    min_version = request_type.value["MinVersion"]
    max_version = request_type.value["MaxVersion"]
    valid_api_versions = list(range(min_version, max_version + 1))
    throttle_time_ms = 0
    error = ErrorCodes.NO_ERROR if request_api_version in valid_api_versions else ErrorCodes.UNSUPPORTED_API_VERSION
    error_code = error.value
    tag_buffer = b"\x00" # Setting to 0 for now
    response_header = correlation_id.to_bytes(4)
    response_body = error_code.to_bytes(2, "big") + int(len(RequestTypes) + 1).to_bytes(1, "big") # num_api_keys
    for req_type in RequestTypes:
        response_body += req_type.value["value"].to_bytes(2, "big") + req_type.value["MinVersion"].to_bytes(2, "big") + req_type.value["MaxVersion"].to_bytes(2, "big") + tag_buffer
    response_body = (
    response_body
    + throttle_time_ms.to_bytes(4, "big")
    + tag_buffer # No tagged fields for entire response
    )

    response = response_header + response_body
    return response

def handle_client(client, client_address):
    print(f"Received connection from {client_address}")
    while True:
        data = client.recv(1024)
        print(f"Received data: {data}")
        kafka_info_dict = parse_kafka_header(data)
        response = make_response(kafka_info_dict)
        message = create_message(response)
        client.sendall(message)

def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    while True:
        client, client_address = server.accept()
        t = threading.Thread(target=handle_client, args=(client, client_address))
        t.start()

if __name__ == "__main__":
    main()
