from fastapi import FastAPI, BackgroundTasks, Query
from queue import Queue, Empty
import threading
from typing import Dict
from app.spider import Spider
from app.domain import get_domain_name_url
from app.general import file_to_set, set_to_file
from fastapi.concurrency import run_in_threadpool

app = FastAPI()

class CrawlerManager:
    def __init__(self, project_name, homepage, domain_name, domain_to_include):
        self.project_name = project_name
        self.homepage = homepage
        self.domain_name = domain_name
        self.domain_to_include = domain_to_include
        self.queue_file = f'temp/{project_name}/queue.txt'
        self.crawled_file = f'temp/{project_name}/crawled.txt'

        self.queue = Queue()
        self.NUMBER_OF_THREADS = 5
        self.CRAWL_LIMIT = 50
        self.threads = []
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.running = False

    def worker(self):
        while not self.stop_event.is_set():
            try:
                url = self.queue.get(timeout=1)
                if url is None:
                    self.queue.task_done()
                    break
                try:
                    Spider.crawl_page(threading.current_thread().name, url)
                except KeyError:
                    pass
                self.queue.task_done()

                print(f"[{self.project_name}] {threading.current_thread().name} - Crawled URL: {url}")
                print(f"[{self.project_name}] Crawled Count: {len(Spider.crawled)}")

                if len(Spider.crawled) >= self.CRAWL_LIMIT:
                    print(f"[{self.project_name}] Crawl limit reached.")
                    self.stop_event.set()
                    break
            except Empty:
                continue
        print(f"[{self.project_name}] {threading.current_thread().name} exiting")

    def create_jobs(self):
        links = file_to_set(self.queue_file)
        for link in links:
            if link not in Spider.crawled and link not in self.queue.queue:
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

        for _ in range(self.NUMBER_OF_THREADS):
            thread = threading.Thread(target=self.worker)
            thread.start()
            self.threads.append(thread)

        print(f"[{self.project_name}] {self.NUMBER_OF_THREADS} threads started.")

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
            "crawled_count": len(Spider.crawled),
            "queue_size": self.queue.qsize(),
            "running": self.running,
            "threads_alive": len([t for t in self.threads if t.is_alive()])
        }


# 🧠 Dictionary to track crawlers per domain
crawlers: Dict[str, CrawlerManager] = {}

@app.get("/")
async def start_crawling(background_tasks: BackgroundTasks, url: str = Query(...)):
    domain = get_domain_name_url(url)

    # If already created, reuse the manager
    if domain not in crawlers:
        project_name = domain
        homepage = url
        domain_name = domain
        domain_to_include = [f'www.{domain}', domain]

        await run_in_threadpool(Spider, project_name, homepage, domain_name, domain_to_include)
        Spider.crawl_page("First Spider", homepage)

        crawlers[domain] = CrawlerManager(project_name, homepage, domain_name, domain_to_include)

    background_tasks.add_task(crawlers[domain].crawl)
    return {"message": f"Crawling started for {url}"}

@app.get("/status")
def get_all_status():
    """Show status for all active crawlers"""
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
