import time
import logging
import starlink_grpc


logging.basicConfig(level=logging.INFO)

def main():
    logging.info("Starlink nav loop starting...")

    while True:
        try:
            # Your logic here
            logging.info("Loop tick...")

            # Example: read Starlink data, update Influx, etc.
            # process_starlink_data()
            starlink_data = starlink_grpc.get_starlink_data()
            
            time.sleep(1)

        except Exception as e:
            logging.error(f"Loop error: {e}")
            time.sleep(2)  # backoff so you don’t spin CPU

if __name__ == "__main__":
    main()
