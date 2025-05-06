from Client import Client
import logging
from Config import Config
import time
import os
import signal

logging.basicConfig(level=logging.INFO)
client_id = os.getenv('CLIENT_ID')
# TODO: Check if this global client is really neccessary
client = None  # Global variable to access client from signal handlers

def signal_handler(sig, frame):
    global client
    logging.info(f"Received signal {sig}, initiating shutdown...")
    if client:
        client.shutdown()

def main():
    global client
    client = Client(name=client_id)
    config = Config()
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        client.connect(config.get_host(), config.get_port())
    except (ConnectionRefusedError, TimeoutError) as e:   
        logging.error(f"Connection error: {e}")
        return
    
    try:
        # Start receiver thread first
        client.start_receiver_thread()
        
        # Define which files to send
        files_to_send = [
            config.get_movies(),
            config.get_credits(),
            config.get_ratings(),
        ]
        
        # Start sender thread and get the thread object
        sender_thread = client.start_sender_thread(files_to_send)
        
        # Wait for sender to finish
        sender_thread.join()
        logging.info("Sender completed. Receiver still active. Press Ctrl+C to exit.")
        
        # Keep main thread running to allow receiver to continue
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        client.shutdown()

if __name__ == '__main__':
    main()
