import grpc
from concurrent import futures
import p2p_pb2
import p2p_pb2_grpc

class TrackerService(p2p_pb2_grpc.TrackerServiceServicer):
    def __init__(self):
        self.peers = {}

    def RegisterPeer(self, request, context):
        self.peers[request.address] = request.files
        return p2p_pb2.RegisterResponse(success=True)

    def QueryPeers(self, request, context):
        peers_with_file = [peer for peer, files in self.peers.items() if request.filename in files]
        return p2p_pb2.PeerList(peers=peers_with_file)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    p2p_pb2_grpc.add_TrackerServiceServicer_to_server(TrackerService(), server)
    server.add_insecure_port('[::]:50053')  # Changed port number
    server.start()
    print("Tracker server started on port 50053")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()