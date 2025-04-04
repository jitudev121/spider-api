import threading
from queue import Queue
from spider import Spider
from domain import get_domain_name_url
from general import file_to_set

# Constants
PROJECT_NAME = 'w3speedup'
HOMEPAGE = 'https://w3speedup.com/'
DOMAIN_NAME = get_domain_name_url(HOMEPAGE)
DOMAIN_TO_INCLUDE = [f'www.{DOMAIN_NAME}', DOMAIN_NAME]
QUEUE_FILE = f'temp/{PROJECT_NAME}/queue.txt'
CRAWLED_FILE = f'temp/{PROJECT_NAME}/crawled.txt'
NUMBER_OF_THREADS = 20
CRAWL_LIMIT = 50

# Initialize Spider
Spider(PROJECT_NAME, HOMEPAGE, DOMAIN_NAME, DOMAIN_TO_INCLUDE)

# Queue for threads
queue = Queue()

# Global counter for crawled pages
crawled_pages_count = 0
crawled_pages_lock = threading.Lock()

def create_workers():
    """Create worker threads."""
    for _ in range(NUMBER_OF_THREADS):
        t = threading.Thread(target=work)
        t.daemon = True
        t.start()

def work():
    """Define the work for each thread."""
    global crawled_pages_count
    while True:
        if crawled_pages_count >= CRAWL_LIMIT:
            break
        url = queue.get()
        try:
            Spider.crawl_page(threading.current_thread().name, url)
            with crawled_pages_lock:
                crawled_pages_count += 1
                if crawled_pages_count >= CRAWL_LIMIT:
                    print("Crawl limit reached. Stopping...")
                    break
        except Exception as e:
            print(f"Error crawling {url}: {e}")
        queue.task_done()

def create_jobs(queue_file):
    """Create jobs for the queue."""
    for link in file_to_set(queue_file):
        queue.put(link)
    queue.join()
    crawl(queue_file)

def crawl(queue_file):
    """Start crawling process."""
    queue_links = file_to_set(queue_file)
    if queue_links:
        print(f"{len(queue_links)} links in the queue")
        create_jobs(queue_file)

# Main execution
if __name__ == "__main__":
    create_workers()
    crawl(QUEUE_FILE)
