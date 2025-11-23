# test_pb2.py
import counter_pb2
import counter_pb2_grpc

print("Testing protobuf imports...")

# This is how you access the message classes
GetRequest = counter_pb2.GetRequest
GetResponse = counter_pb2.GetResponse
IncrementRequest = counter_pb2.IncrementRequest
SetRequest = counter_pb2.SetRequest
CompareAndSetRequest = counter_pb2.CompareAndSetRequest

# Test creating instances
request = GetRequest()
print(f"✓ GetRequest created: {request}")

inc_req = IncrementRequest(amount=5)
print(f"✓ IncrementRequest created: {inc_req}")

set_req = SetRequest(value=100)
print(f"✓ SetRequest created: {set_req}")

cas_req = CompareAndSetRequest(expected=10, value=20)
print(f"✓ CompareAndSetRequest created: {cas_req}")

print("\n✓ All protobuf classes work correctly!")
