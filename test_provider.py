import pytest
from pathlib import Path
from pact import Verifier
from pact.types import Message
from app1.src.main import handle_ping, PingMessage

PACTS_DIR = Path(__file__).parent / "pacts"

# 1. Define verification handlers for your contract scenarios.
# The key must match the description string used in your consumer test's `.upon_receiving()`
def message_producer_callback(name: str, metadata: dict | None = None, **kwargs) -> Message:
    """
    Callback to produce the message payload which will be verified against the contract provided by the consumer.
    """
    if name == "A pong message matching the standard cycle schema":
        import asyncio

        # Simulate incoming mock event state the triggers App1's business logic
        mock_incoming_ping = PingMessage(counter=1)

        # Execute App1's actual production logic (handling the async function execution)
        response_msg = asyncio.run(handle_ping(mock_incoming_ping))

        # Return a Pact Message container (contents must be raw bytes)
        return Message(
            contents=response_msg.model_dump_json().encode("utf-8"),
            content_type="application/json",
            metadata={"topic": "app1-buff-output"}
        )
    
    raise ValueError(f"Unknown interaction description: '{name}'")

def test_verify_provider_against_contract():
    """
    Executes the modern Pact Verifier using fluent builders against local files.
    """
    contract_file = PACTS_DIR / "App2-Consumer-App1-Provider.json"

    if not contract_file.exists():
        pytest.fail(f"Contract file not found at {contract_file}. Run consumer tests first.")
    
    # 2. Configure the Verifier object
    verifier = (
        Verifier(name="App1-Provider")
        .add_source(str(contract_file))
        .message_handler(message_producer_callback)
    )

    # Executes the contract validation tests
    verifier.verify()
