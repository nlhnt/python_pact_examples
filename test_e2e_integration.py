import pytest
import asyncio
from faststream.kafka import TestKafkaBroker

# import the brokers and message schemas from your separate apps
from app1.src.main import broker as broker1, PingMessage
from app2.src.main import broker as broker2

# Define a custom exception to cleanly pop out of the loop
class CircuitBreakerException(Exception):
    pass

@pytest.mark.asyncio
async def test_complete_e2e_ping_pong_loop():
    """
    Executes a multi-broker in-memory test but includes a circuit breaker
    to halt the infinite message processing pipeline before hitting OOM.
    """
    # we prepare a list that we'll see how many messages will be received by our broker
    captured_messages = []

    @broker1.subscriber("app1-buff-input")
    async def spy_handler(msg: PingMessage):
        captured_messages.append(msg.counter)
        print(f"\ncaptured_messages: {captured_messages}")
        # CIRCUIT BREAKER: Stop the infinite cycle as soon as we prove 
        # a full roundtrip occurred (e.g., when the counter hits 3)
        if msg.counter >= 10:
            print("[SPY] Circuit breaker triggered! Killing infinite loop.")
            raise CircuitBreakerException("Target counter reached.")

    # 1. start up both tests brokers. This should link their internal routing loops
    # async with TestKafkaBroker(broker1) as br1, TestKafkaBroker(broker2) as br2:
    async with TestKafkaBroker(broker1, broker2) as (br1, br2):
        # async with TestApp(unified_app):
        # 2. Kickstart the loop by publishing an initial event to App1's input topic
        try:
            await br1.publish(
                PingMessage(counter=1),
                topic="app1-buff-input"
            )

            # 3. Give the in-memory event loop a brief moment to process the round-trip
            # Since it's all running on a local memory micro-tasks it should be really quick.
            await asyncio.sleep(3)

            # If your applications have some logging you should see the logs now
            # That's if you run it using these flags: pytest -vvv -s
        except CircuitBreakerException:
            # Catch the hacked exception to prevent max recursion error
            pass

        # Here's a place you can define your own assert
        # assert True
        assert len(captured_messages) > 0
        assert 3 in captured_messages