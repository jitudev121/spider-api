from fastapi import FastAPI, BackgroundTasks
from queue import Queue, Empty
import threading
from typing import Dict
from app.spider import Spider
from app.domain import get_domain_name_url
from app.general import file_to_set, set_to_file
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

app = FastAPI()

class CrawlerManager:
    def __init__(self, project_name, homepage, domain_name, domain_to_include, number_of_threads=2, crawl_limit=10):
        self.project_name = project_name
        self.homepage = homepage
        self.domain_name = domain_name
        self.domain_to_include = domain_to_include
        self.queue_file = f'temp/{project_name}/queue.txt'
        self.crawled_file = f'temp/{project_name}/crawled.txt'

        self.queue = Queue()
        self.number_of_threads = number_of_threads
        self.crawl_limit = crawl_limit
        self.threads = []
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.running = False

        # Create a separate Spider instance for each crawler
        self.spider = Spider(project_name, homepage, domain_name, domain_to_include)
        
    def worker(self):
        while not self.stop_event.is_set():
            try:
                url = self.queue.get(timeout=1)
                    
                if len(self.spider.crawled) >= self.crawl_limit:
                    print(f"[{self.project_name}] Crawl limit reached.")

                    self.stop_event.set()
                    break

                if url is None:
                    self.queue.task_done()
                    break
                try:
                    self.spider.crawl_page(threading.current_thread().name, url)
                except Exception as e:
                    print(f"[{self.project_name}] Error: {e}")
                self.queue.task_done()

                print(f"[{self.project_name}] {threading.current_thread().name} - Crawled URL: {url}")
                print(f"[{self.project_name}] Crawled Count: {len(self.spider.crawled)}")
 
            except Empty:
                continue
        print(f"[{self.project_name}] {threading.current_thread().name} exiting")

    def create_jobs(self):
        links = file_to_set(self.queue_file)
        for link in links:
            if link not in self.spider.crawled and link not in self.queue.queue:
                self.queue.put(link)
        print(f"[{self.project_name}] Jobs created.")

    def crawl(self):
        if self.running:
            print(f"[{self.project_name}] Already crawling.")
            return

        with self.lock:
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

        while not self.queue.empty():
            try:
                self.queue.get_nowait()
                self.queue.task_done()
            except Empty:
                break

        for thread in self.threads:
            thread.join()

        self.threads = []
        self.running = False
        self.update_queue_file()
        print(f"[{self.project_name}] Crawling stopped.")

    def update_queue_file(self):
        remaining_links = list(self.queue.queue)
        set_to_file(set(remaining_links), self.queue_file)

    def status(self):
        return {
            "project": self.project_name,
            "crawled_count": len(self.spider.crawled),
            "queue_size": self.queue.qsize(),
            "running": self.running,
            "threads_alive": len([t for t in self.threads if t.is_alive()])
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

        # Init manager
        crawler = CrawlerManager(project_name, homepage, domain_name, domain_to_include, number_of_threads, crawl_limit)
        crawler.spider.boot()  # Boot only after spider instance exists
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
