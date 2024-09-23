import grpc
from concurrent import futures
import threading
import p2p_pb2
import p2p_pb2_grpc
import os
import sys
import socket
import queue

class P2PService(p2p_pb2_grpc.P2PServiceServicer):
    def UploadFile(self, request, context):
        os.makedirs('uploads', exist_ok=True)  # Ensure the uploads directory exists
        file_path = os.path.join('uploads', f"{request.filename}.part{request.chunk_number}")
        with open(file_path, 'wb') as f:
            f.write(request.data)
        return p2p_pb2.UploadResponse(success=True)

    def DownloadFile(self, request, context):
        file_path = os.path.join('uploads', f"{request.filename}.part{request.chunk_number}")
        try:
            with open(file_path, 'rb') as f:
                data = f.read()
            yield p2p_pb2.FileChunk(filename=request.filename, data=data, chunk_number=request.chunk_number)
        except FileNotFoundError:
            context.set_details(f"File chunk {request.filename}.part{request.chunk_number} not found")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return

def serve(port_queue):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    p2p_pb2_grpc.add_P2PServiceServicer_to_server(P2PService(), server)
    port = server.add_insecure_port('[::]:0')  # Bind to an available port
    server.start()
    port_queue.put(port)
    server.wait_for_termination()

def register_peer(tracker_stub, address, files):
    peer_info = p2p_pb2.PeerInfo(address=address, files=files)
    tracker_stub.RegisterPeer(peer_info)

def upload_file(tracker_stub, file_path):
    filename = os.path.basename(file_path)
    with open(file_path, 'rb') as f:
        content = f.read()
    file_info = p2p_pb2.FileInfo(filename=filename, content=content)
    tracker_stub.UploadFile(file_info)

def retrieve_file(tracker_stub, filename):
    file_request = p2p_pb2.FileRequest(filename=filename)
    file_info = tracker_stub.RetrieveFile(file_request)
    if file_info:
        with open(filename, 'wb') as f:
            f.write(file_info.content)
        print(f"File {filename} retrieved successfully.")
    else:
        print(f"File {filename} could not be retrieved.")

def discover_peers(tracker_stub, filename):
    file_request = p2p_pb2.FileRequest(filename=filename)
    peer_list = tracker_stub.DiscoverPeers(file_request)
    return [peer.address for peer in peer_list.peers]

def interactive_mode(tracker_stub):
    while True:
        command = input("Enter command (upload, download, search, exit): ").strip().lower()
        if command == "upload":
            file_path = input("Enter the file path to upload: ").strip().strip('"')
            file_path = os.path.normpath(file_path)  # Normalize the file path
            if os.path.exists(file_path):
                upload_file(tracker_stub, file_path)
                print(f"File {file_path} uploaded and registered.")
            else:
                print(f"File {file_path} does not exist.")
        elif command == "download":
            filename = input("Enter the filename to download: ").strip()
            retrieve_file(tracker_stub, filename)
        elif command == "search":
            filename = input("Enter the filename to search: ").strip()
            peers = discover_peers(tracker_stub, filename)
            if peers:
                print(f"Peers with the file {filename}: {peers}")
            else:
                print(f"File {filename} not found in the network.")
        elif command == "exit":
            break
        else:
            print("Unknown command. Please enter 'upload', 'download', 'search', or 'exit'.")

def main():
    global port

    # Create a queue to get the port number from the server thread
    port_queue = queue.Queue()

    # Start the server in a separate thread and get the port
    server_thread = threading.Thread(target=serve, args=(port_queue,))
    server_thread.start()

    # Get the port number from the queue
    port = port_queue.get()

    # Connect to the tracker
    tracker_channel = grpc.insecure_channel('172.31.86.16:50053')  # Updated to use the private IP address
    tracker_stub = p2p_pb2_grpc.TrackerServiceStub(tracker_channel)

    # Register the peer with the tracker
    address = f'localhost:{port}'
    files = []  # Initially, no files are registered
    register_peer(tracker_stub, address, files)

    # Enter interactive mode
    interactive_mode(tracker_stub)

if __name__ == '__main__':
    main()