import asyncio
import datetime
from prefect.agent import PrefectAgent


async def run_agent():
    agent = PrefectAgent(["default"])
    await agent.start()
    print(f"Agent started at {datetime.datetime.utcnow()}")
    while True:
        await asyncio.sleep(5)


if __name__ == '__main__':
    asyncio.run(run_agent())
