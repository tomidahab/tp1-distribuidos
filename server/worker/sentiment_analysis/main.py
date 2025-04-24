import asyncio
import logging
from worker import SentimentWorker

async def main():
    """Main entry point for the sentiment analysis worker service"""
    # Add retry logic for service initialization
    retry_count = 0
    
    while True:
        try:
            worker = SentimentWorker()
            success = await worker.run()
            
            if success:
                break  # Worker completed successfully
            else:
                logging.error("Sentiment worker failed to run properly")
                retry_count += 1
                
        except Exception as e:
            retry_count += 1
            logging.error(f"Error running sentiment worker: {e}. Retry {retry_count}")

        wait_time = 2 ** retry_count  # Exponential backoff
        logging.info(f"Waiting {wait_time} seconds before retrying...")
        await asyncio.sleep(wait_time)


if __name__ == "__main__":
    logging.info("Starting sentiment analysis worker service...")
    asyncio.run(main())