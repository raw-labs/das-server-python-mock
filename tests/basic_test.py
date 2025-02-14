# A bit hacky but this also allows Python to see com/ without having to do pip install -e . for local dev...
import sys, os
sys.path.insert(0, os.path.abspath("."))

import time
import threading
import grpc
import pytest

# Import the generated stubs for the HealthCheckService.
from com.rawlabs.protocol.das.v1.services import (
    health_service_pb2,
    health_service_pb2_grpc,
)
# Import the server's entry point
from das_mock.server import serve

# Use a different port for testing to avoid conflicts with any production server.
TEST_PORT = 50052

@pytest.fixture(scope="module")
def grpc_server():
    """
    Starts the gRPC server in a background thread.
    """
    # Start the server in a daemon thread
    server_thread = threading.Thread(target=serve, args=(TEST_PORT,), daemon=True)
    server_thread.start()
    # Wait a moment to allow the server to start up
    time.sleep(2)
    yield
    # Normally, you would signal the server to shutdown gracefully.
    # Since our serve() loop is blocking, and the thread is daemonized,
    # it will exit when the test process terminates.

def test_health_check(grpc_server):
    """
    Tests the HealthCheckService by calling its Check method.
    """
    # Create an insecure channel to the test server
    channel = grpc.insecure_channel(f"localhost:{TEST_PORT}")
    stub = health_service_pb2_grpc.HealthCheckServiceStub(channel)
    
    # Create an empty request
    request = health_service_pb2.HealthCheckRequest()
    
    # Call the Check RPC
    response = stub.Check(request)
    
    # Assert that the status is SERVING (as per our mock implementation)
    assert response.status == health_service_pb2.HealthCheckResponse.ServingStatus.SERVING
    # Optionally, check the description string
    assert "healthy" in response.description.lower()