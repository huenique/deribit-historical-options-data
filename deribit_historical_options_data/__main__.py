import csv
import json
import logging
import threading
from queue import Queue
from urllib import error, request

CURRENCY = "BTC"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("runtime.log"), logging.StreamHandler()],
)


def fetch_instruments() -> list[str]:
    logging.info("Fetching instrument names...")
    url = f"https://history.deribit.com/api/v2/public/get_instruments?currency={CURRENCY}&kind=option&expired=true&include_old=true&count=10000"
    try:
        with request.urlopen(url) as response:
            data = json.loads(response.read().decode())
            instrument_names = [item["instrument_name"] for item in data["result"]]
            logging.info(f"Fetched {len(instrument_names)} instrument names.")
            return instrument_names
    except error.URLError as e:
        logging.error(f"Error fetching instruments: {e}")
        return []


def save_instruments_to_csv(instrument_names: list[str], filename: str):
    logging.info(f"Saving instrument names to {filename}...")
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file)
        for name in instrument_names:
            writer.writerow([name])
    logging.info("Instrument names saved.")


def worker(queue: Queue[str], filename: str, lock: threading.Lock):
    while not queue.empty():
        instrument_name = queue.get()
        url = f"https://www.deribit.com/api/v2/public/get_order_book?instrument_name={instrument_name}"
        try:
            with request.urlopen(url) as response:
                data = json.loads(response.read().decode())
                result_data = data.get("result", {})
                if result_data:
                    with lock:
                        with open(filename, "a", newline="") as file:
                            writer = csv.DictWriter(file, fieldnames=result_data.keys())
                            if file.tell() == 0:  # Write header only if file is empty
                                writer.writeheader()
                            writer.writerow(result_data)
                    logging.info(f"Fetched and saved data for {instrument_name}")
        except error.URLError as e:
            logging.error(f"Error fetching data for {instrument_name}: {e}")
        queue.task_done()


def fetch_instrument_data(instrument_names: list[str], filename: str):
    logging.info("Fetching data for each instrument...")
    queue: Queue[str] = Queue()
    lock = threading.Lock()

    for name in instrument_names:
        queue.put(name)

    threads = [
        threading.Thread(target=worker, args=(queue, filename, lock)) for _ in range(12)
    ]
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    logging.info("All instrument data fetched.")


def main():
    instrument_names = fetch_instruments()
    save_instruments_to_csv(instrument_names, "instrument_names.csv")
    fetch_instrument_data(instrument_names, "instrument_data.csv")


if __name__ == "__main__":
    main()
