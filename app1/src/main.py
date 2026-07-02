from pydantic import BaseModel, Field
from faststream import FastStream
from faststream.kafka import KafkaBroker

# 1. Define explicit schemas for contract matching
class PingMessage(BaseModel):
    message_type: str = Field(default="ping")
    counter: int

class PongMessage(BaseModel):
    message_type: str = Field(default="pong")
    counter: int

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

# Publisher registration
pong_publisher = broker.publisher("app1-buff-output")

@broker.subscriber("app1-buff-input")
@pong_publisher
async def handle_ping(msg: PingMessage) -> PongMessage:
    print(f"App1 received: {msg.message_type} ({msg.counter})")
    # Return automatically routes to the registered publisher topic
    return PongMessage(counter=msg.counter + 1)