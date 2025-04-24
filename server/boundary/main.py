# main.py
import asyncio
import logging
import time
from Boundary import Boundary

async def main():
    retry_count = 0
    
    while True:
        try:
            server = Boundary(port=5000)
            await server.run()
            break
        except Exception as e:
            retry_count += 1
            logging.error(f"Error running server: {e}. Retry {retry_count}")
            wait_time = 2 ** retry_count
            logging.info(f"Waiting {wait_time} seconds before retrying...")
            await asyncio.sleep(wait_time)

if __name__ == "__main__":
    asyncio.run(main())
    