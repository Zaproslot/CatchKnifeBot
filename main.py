import asyncio
from Processor import MainProcessor


mp = MainProcessor()

if __name__ == "__main__":
    asyncio.run(mp.run())
