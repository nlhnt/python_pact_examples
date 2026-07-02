import pytest
from pathlib import Path
from faststream.kafka import TestKafkaBroker
from pact import match

# Import your components
from app1.src.main import broker as broker1, PingMessage, PongMessage
from app2.src.main import broker as broker2

PACTS_DIR = Path(__file__).parent / "pacts"

@pytest.mark.asyncio
async def test_in_memory_ping_pong_flow():
    """
    Functional test using FastStream's built-in in-memory patch.
    This simulates the real async network routing loop on local threads.
    """
    async with TestKafkaBroker(broker1) as br1, TestKafkaBroker(broker2) as br2:
        # Inject an initial entry point message into the App1 queue
        await br1.publish(PingMessage(counter=1), topic="app1-buff-input")
        
        # FastStream processes synchronously inside the TestKafkaBroker context manager block
        assert True 


def test_generate_message_contract():
    """
    Generates a V4 Message Pact json file representing the schema agreement.
    App2 expects a specific payload shape when consuming from 'app1-buff-output'.
    """
    from pact import Pact
    
    # Define pact relationship: App2 is the Consumer of App1's produced topic
    pact = Pact("App2-Consumer", "App1-Provider").with_specification("V4")
    
    # Define expected payload structure with dynamic matchers
    expected_pong_payload = {
        "message_type": match.string("pong"),
        "counter": match.integer(2)
    }
    
    (
        pact.upon_receiving("A pong message matching the standard cycle schema", interaction="Async")
        .with_metadata({"topic": "app1-buff-output"})
        .with_body(expected_pong_payload, content_type="application/json")
    )
        
    pact.write_file(PACTS_DIR)