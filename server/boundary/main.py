import asyncio
import logging
import os
import sys
from Boundary import Boundary
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)

async def main():
    # Load environment variables
    load_dotenv()
    
    # Get port from environment or use default
    port = int(os.getenv("PORT", 5000))
    
    # Router queue names from environment variables
    movies_router_queue = os.getenv("MOVIES_ROUTER_QUEUE")
    movies_router_q5_queue = os.getenv("MOVIES_ROUTER_Q5_QUEUE")
    credits_router_queue = os.getenv("CREDITS_ROUTER_QUEUE")
    ratings_router_queue = os.getenv("RATINGS_ROUTER_QUEUE")

    retry_count = 0
    
    while True:
        try:
            server = Boundary(port, 100, movies_router_queue, credits_router_queue, ratings_router_queue)
            server.movies_router_q5_queue = movies_router_q5_queue
            await server.run()
            break
        except Exception as e:
            retry_count += 1
            logging.error(f"Error running server: {e}. Retry {retry_count}")
            wait_time = min(30, 2 ** retry_count)  # Cap the wait time at 30 seconds
            logging.info(f"Waiting {wait_time} seconds before retrying...")
            await asyncio.sleep(wait_time)

if __name__ == "__main__":
    asyncio.run(main())
