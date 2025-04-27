import asyncio
import logging
import os
import sys
from dotenv import load_dotenv
from Worker import RouterWorker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

async def main():
    """Main function to run the gateway worker"""
    load_dotenv()
    
    number_of_producer_workers = int(os.getenv("NUMBER_OF_PRODUCER_WORKERS"))
    input_queue = os.getenv("INPUT_QUEUE")
    output_queues_str = os.getenv("OUTPUT_QUEUES")
    exchange_name = os.getenv("EXCHANGE_NAME")
    exchange_type = os.getenv("EXCHANGE_TYPE", "direct")
    balancer_type = os.getenv("BALANCER_TYPE", "round_robin")

    if not number_of_producer_workers or not input_queue or not output_queues_str or not exchange_name:
        logging.error("Required environment variables are not set!")
        return

    output_queues = output_queues_str.split(',') if output_queues_str else []
    
    if not input_queue:
        logging.error("INPUT_QUEUE environment variable must be set")
        return
        
    if not output_queues:
        logging.error("OUTPUT_QUEUES environment variable must be set")
        return
        
    logging.info(f"Starting gateway with input queue: {input_queue}")
    logging.info(f"Output queues: {output_queues}")
    logging.info(f"Using exchange: {exchange_name} ({exchange_type})")
    
    worker = RouterWorker(
        number_of_producer_workers=number_of_producer_workers,
        input_queue=input_queue,
        output_queues=output_queues,
        exchange_name=exchange_name,
        exchange_type=exchange_type,
        balancer_type=balancer_type
    )
    await worker.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Gateway shutting down")
