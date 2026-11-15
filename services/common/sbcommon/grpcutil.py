"""gRPC server helper for 2PC participants (with Prometheus metrics)."""
import logging
from concurrent import futures

import grpc
from prometheus_client import Counter

from .config import SERVICE_NAME

log = logging.getLogger("grpc")

TWOPC_PARTICIPANT_CALLS = Counter(
    "twopc_participant_calls_total", "2PC calls handled by this participant", ["service", "phase", "result"]
)


def start_participant_server(servicer, port: int) -> grpc.Server:
    from sbproto import twopc_pb2_grpc

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    twopc_pb2_grpc.add_ParticipantServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    log.info("gRPC 2PC participant listening", extra={"port": port, "participant": SERVICE_NAME})
    return server
