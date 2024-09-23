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
    print(f"Peer server started on port {port}")
    port_queue.put(port)  # Put the port number in the queue
    server.wait_for_termination()

def partition_file(file_path, chunk_size=1024):
    with open(file_path, 'rb') as f:
        chunk_number = 0
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk_number, chunk
            chunk_number += 1

def upload_file(tracker_stub, file_path):
    peers = discover_peers(tracker_stub, os.path.basename(file_path))
    if not peers:
        print("No peers available to upload the file.")
        return

    peer_stubs = [p2p_pb2_grpc.P2PServiceStub(grpc.insecure_channel(peer)) for peer in peers]
    num_peers = len(peer_stubs)

    for chunk_number, chunk in partition_file(file_path):
        peer_stub = peer_stubs[chunk_number % num_peers]
        file_chunk = p2p_pb2.FileChunk(filename=os.path.basename(file_path), data=chunk, chunk_number=chunk_number)
        response = peer_stub.UploadFile(file_chunk)
        if not response.success:
            print(f"Failed to upload chunk {chunk_number} to peer {peers[chunk_number % num_peers]}")

    # Register the file chunks with the tracker
    for i, peer in enumerate(peers):
        peer_stub = peer_stubs[i]
        file_info = p2p_pb2.FileInfo(filename=os.path.basename(file_path), peer_address=peer)
        tracker_stub.RegisterFile(file_info)

def download_file(stub, filename, chunk_number):
    request = p2p_pb2.FileRequest(filename=filename, chunk_number=chunk_number)
    response = stub.DownloadFile(request)
    for file_chunk in response:
        os.makedirs('downloads', exist_ok=True)  # Ensure the downloads directory exists
        file_path = os.path.join('downloads', f"{filename}.part{file_chunk.chunk_number}")
        with open(file_path, 'wb') as f:
            f.write(file_chunk.data)

def discover_peers(tracker_stub, filename):
    request = p2p_pb2.FileRequest(filename=filename)
    response = tracker_stub.QueryPeers(request)
    return response.peers

def register_peer(tracker_stub, address, files):
    peer_info = p2p_pb2.PeerInfo(address=address, files=files)
    response = tracker_stub.RegisterPeer(peer_info)
    return response.success

def synchronize_files(tracker_stub, peer_stub, filename):
    peers = discover_peers(tracker_stub, filename)
    for peer_address in peers:
        if peer_address != f'localhost:{port}':  # Skip self
            peer_channel = grpc.insecure_channel(peer_address)
            peer_stub = p2p_pb2_grpc.P2PServiceStub(peer_channel)
            chunk_number = 0
            while True:
                try:
                    download_file(peer_stub, filename, chunk_number)
                    chunk_number += 1
                except:
                    break

def replicate_file(tracker_stub, filename, replication_factor=3):
    peers = discover_peers(tracker_stub, filename)
    for chunk_number in range(len(peers)):
        for _ in range(replication_factor - 1):
            for peer_address in peers:
                if peer_address != f'localhost:{port}':  # Skip self
                    peer_channel = grpc.insecure_channel(peer_address)
                    peer_stub = p2p_pb2_grpc.P2PServiceStub(peer_channel)
                    download_file(peer_stub, filename, chunk_number)
                    upload_file(peer_stub, f"{filename}.part{chunk_number}")

def search_file(tracker_stub, filename):
    peers = discover_peers(tracker_stub, filename)
    if not peers:
        print(f"File {filename} not found in the network.")
    return peers

def retrieve_file(tracker_stub, filename):
    peers = search_file(tracker_stub, filename)
    if peers:
        for peer_address in peers:
            peer_channel = grpc.insecure_channel(peer_address)
            peer_stub = p2p_pb2_grpc.P2PServiceStub(peer_channel)
            chunk_number = 0
            while True:
                try:
                    download_file(peer_stub, filename, chunk_number)
                    chunk_number += 1
                except:
                    break
        print(f"File {filename} retrieved successfully.")
    else:
        print(f"File {filename} could not be retrieved.")

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
    tracker_channel = grpc.insecure_channel('localhost:50053')  # Ensure this matches the tracker's port
    tracker_stub = p2p_pb2_grpc.TrackerServiceStub(tracker_channel)

    # Register the peer with the tracker
    address = f'localhost:{port}'
    files = []  # Initially, no files are registered
    register_peer(tracker_stub, address, files)

    # Enter interactive mode
    interactive_mode(tracker_stub)

if __name__ == '__main__':
    main()