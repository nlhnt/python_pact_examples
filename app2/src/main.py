from pydantic import BaseModel, Field
from faststream import FastStream
from faststream.kafka import KafkaBroker

class PingMessage(BaseModel):
    message_type: str = Field(default="ping")
    counter: int

class PongMessage(BaseModel):
    message_type: str = Field(default="pong")
    counter: int

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

ping_publisher = broker.publisher("app1-buff-input")

@broker.subscriber("app1-buff-output")
@ping_publisher
async def handle_pong(msg: PongMessage) -> PingMessage:
    print(f"App2 received: {msg.message_type} ({msg.counter})")
    return PingMessage(counter=msg.counter + 1)