from Client import Client
import logging
from Config import Config

logging.basicConfig(level=logging.INFO)

def main():
    client = Client(name="John Doe", age=30)
    config = Config()
    try:
        client.connect(config.get_host(), config.get_port())
    except (ConnectionRefusedError, TimeoutError) as e:   
        logging.error(f"Connection error: {e}")
        return
    
    try:
        client.send_queries([config.get_query1(), config.get_query2()])
        data = client.recv_response()
        logging.info(f"Received data: {data}")
    except OSError as e:
        logging.error(f"OS error: {e}")
        
    except Exception as e:
        logging.error(f"Error: {e}")
        
    finally:
        client.shutdown()

if __name__ == '__main__':
    main()