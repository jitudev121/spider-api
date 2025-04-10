from fastapi import FastAPI, BackgroundTasks
from queue import Queue, Empty
import threading
from typing import Dict
from app.spider import Spider
from app.domain import get_domain_name_url
from pydantic import BaseModel
import redis

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
app = FastAPI()

class CrawlerManager:
    def __init__(self, project_name, homepage, domain_name, domain_to_include, number_of_threads=2, crawl_limit=10):
        self.project_name = project_name
        self.homepage = homepage
        self.domain_name = domain_name
        self.domain_to_include = domain_to_include

        self.queue = Queue()
        self.number_of_threads = number_of_threads
        self.crawl_limit = crawl_limit
        self.threads = []
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.running = False
        self.spider = Spider(project_name, homepage, domain_name, domain_to_include)

    def worker(self):
        thread_name = threading.current_thread().name

        while not self.stop_event.is_set():

            try:
                url = self.queue.get(timeout=1)
            except Empty:
                continue
            except Exception as e:
                print(f"Error getting from queue: {e}")
                continue

            try:
                if not url or url in self.spider.crawled:
                    self.queue.task_done()  # Mark the task as done even if it's already crawled or invalid
                    continue
                if len(self.spider.crawled) >= self.crawl_limit:
                    print(f"[{self.project_name}] Crawl limit reached.")
                    self.stop_event.set()
                    self.queue.task_done()  # Ensure task_done is called when stopping
                    return

                try:
                    self.spider.crawl_page(thread_name, url)
                    with self.lock:  # Assuming you have a lock for thread safety
                        self.spider.crawled.add(url)
                    print(f"[{self.project_name}] {thread_name} - Crawled URL: {url}")
                    print(f"[{self.project_name}] Crawled Count: {len(self.spider.crawled)}")

                except Exception as e:
                    print(f"[{self.project_name}] Error crawling {url}: {e}")
                
            finally:
                # Ensure that task_done() is always called no matter what
                self.queue.task_done()
                
            
            # Optionally, check if the queue is empty and no tasks are unfinished
            if self.queue.empty() and self.queue.unfinished_tasks == 0:

                redis_queue_key = f"{self.project_name}:queue"
                links = redis_client.smembers(redis_queue_key)
                current_queue = set(self.queue.queue)
                for link in links:
                    if link not in self.spider.crawled and link not in current_queue:
                        self.queue.put(link)

                if self.queue.empty():
                    self.create_jobs()
                    print(f"[{self.project_name}] All tasks done. Stopping...")
                    self.stop_event.set()
                    return

        print(f"[{self.project_name}] {thread_name} exiting")

    def create_jobs(self):
        redis_queue_key = f"{self.project_name}:queue"
        links = redis_client.smembers(redis_queue_key)
        
        if not links:
            print(f"[{self.project_name}] No links found in Redis.")
            return

        # # Decode bytes to string if needed
        # links = {link.decode('utf-8') if isinstance(link, bytes) else link for link in links}

        current_queue = set(self.queue.queue)
        for link in links:
            if link not in self.spider.crawled and link not in current_queue:
                self.queue.put(link)

        print(f"[{self.project_name}] Jobs created from Redis.")

    def crawl(self):
        with self.lock:
            if self.running:
                print(f"[{self.project_name}] Already crawling.")
                return
        
            self.running = True
            self.stop_event.clear()

        self.create_jobs()
        self.threads = []

        for _ in range(self.number_of_threads):
            thread = threading.Thread(target=self.worker)
            thread.start()
            self.threads.append(thread)

        print(f"[{self.project_name}] {self.number_of_threads} threads started.")

    def stop_crawling(self):
        self.stop_event.set()

        remaining_links = []

        while not self.queue.empty():
            try:
                link = self.queue.get_nowait()
                remaining_links.append(link)
                self.queue.task_done()
            except Empty:
                break

        for thread in self.threads:
            thread.join()

        self.threads = []
        self.running = False
        self.update_queue_in_redis(remaining_links)
        print(f"[{self.project_name}] Crawling stopped.")

    def update_queue_in_redis(self, links):
        redis_queue_key = f"{self.project_name}:queue"
        if links:
            redis_client.sadd(redis_queue_key, *links)

    def status(self):
        return {
            "project": self.project_name,
            "crawled_count": len(self.spider.crawled),
            "queue_size": self.queue.qsize(),
            "running": self.running,
            "threads_alive": sum(t.is_alive() for t in self.threads)
        }


# Manage crawlers for multiple domains
crawlers: Dict[str, CrawlerManager] = {}

class CrawlRequest(BaseModel):
    url: str
    number_of_threads: int
    crawl_limit: int

@app.get("/")
async def start_crawling(request: CrawlRequest, background_tasks: BackgroundTasks):
    url = request.url
    number_of_threads = request.number_of_threads
    crawl_limit = request.crawl_limit
    domain = get_domain_name_url(url)

    if domain not in crawlers:
        project_name = domain
        homepage = url
        domain_name = domain
        domain_to_include = [f'www.{domain}', domain]

        crawler = CrawlerManager(project_name, homepage, domain_name, domain_to_include, number_of_threads, crawl_limit)
        crawler.spider.crawl_page("First Spider", homepage)
        crawlers[domain] = crawler

    background_tasks.add_task(crawlers[domain].crawl)
    return {"message": f"Crawling started for {url}"}

@app.get("/status")
def get_all_status():
    return {domain: crawler.status() for domain, crawler in crawlers.items()}

@app.get("/status/{domain}")
def get_status(domain: str):
    if domain not in crawlers:
        return {"error": "No such crawler running"}
    return crawlers[domain].status()

@app.post("/stop/{domain}")
def stop(domain: str):
    if domain in crawlers:
        crawlers[domain].stop_crawling()
        return {"message": f"Crawling stopped for {domain}"}
    return {"error": "Crawler not found"}
