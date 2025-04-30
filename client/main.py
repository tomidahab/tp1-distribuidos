from Client import Client
import logging
from Config import Config
import os
from time import sleep
import time 

logging.basicConfig(level=logging.INFO)

CLIENT_ID = os.getenv("CLIENT_ID")

def main():
    sleep(90)
    client = Client(name=CLIENT_ID)
    config = Config()
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
        
        sleep(1)
        client._handle_sigterm(None,None)
        # Wait for sender to finish
        sender_thread.join()
        logging.info("Sender completed. Receiver still active. Press Ctrl+C to exit.")
        
        # Keep main thread running to allow receiver to continue
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Received interrupt, shutting down...")
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        client.shutdown()

if __name__ == '__main__':
    main()