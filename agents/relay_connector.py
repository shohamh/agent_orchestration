"""Run the relay connector on an execution PC (outbound only; see agents/flowkit/connector.py)."""
import asyncio
import logging
from flowkit.connector import from_environment

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(message)s')
    asyncio.run(from_environment().run())
