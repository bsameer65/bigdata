import asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "processed_radiation_data"
GROUP_ID = "fastapi-consumer-group"

# Store WebSocket connections
active_connections: list[WebSocket] = []

# Broadcast Kafka messages to all WebSocket clients
async def broadcast_message(message: str):
    disconnected_clients = []
    for connection in active_connections:
        try:
            await connection.send_text(message)
        except Exception as e:
            print(f"❌ Failed to send message: {e}")
            disconnected_clients.append(connection)

    # Remove disconnected clients
    for dc in disconnected_clients:
        active_connections.remove(dc)

# Kafka consumer coroutine with retry logic
async def consume_kafka():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
    )

    # Retry connection to Kafka until successful
    while True:
        try:
            await consumer.start()
            print("✅ Connected to Kafka")
            break
        except KafkaConnectionError as e:
            print("⏳ Kafka not ready, retrying in 5 seconds...")
            await asyncio.sleep(5)

    try:
        async for msg in consumer:
            decoded_message = msg.value.decode("utf-8")
            print(f"[Kafka] {decoded_message}")
            await broadcast_message(decoded_message)
    finally:
        await consumer.stop()

# WebSocket endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    print("✅ Client connected.")
    try:
        while True:
            await websocket.receive_text()  # Keep connection alive
    except WebSocketDisconnect:
        print("⚠️ Client disconnected.")
        active_connections.remove(websocket)

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "OK"}

# Start Kafka consumer in background on FastAPI startup
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(consume_kafka())
