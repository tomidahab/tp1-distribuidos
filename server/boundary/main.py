# main.py
import asyncio
import logging
import time
from Boundary import Boundary

async def main():
    # Add retry logic for service initialization
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            server = Boundary(port=5000)
            await server.run()
            break  # If run completes successfully, exit the loop
        except Exception as e:
            retry_count += 1
            logging.error(f"Error running server: {e}. Retry {retry_count}/{max_retries}")
            if retry_count < max_retries:
                wait_time = 2 ** retry_count  # Exponential backoff
                logging.info(f"Waiting {wait_time} seconds before retrying...")
                await asyncio.sleep(wait_time)  # Use asyncio.sleep instead of time.sleep
            else:
                logging.error("Maximum retries reached. Exiting.")
                raise

if __name__ == "__main__":
    asyncio.run(main())
    