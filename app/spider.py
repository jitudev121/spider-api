import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import re
import redis

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

class Spider:
    def __init__(self, project_name, base_url, domain_name, domain_to_include):
        self.project_name = project_name
        self.base_url = base_url
        self.domain_name = domain_name
        self.domain_to_include = domain_to_include

        self.queue_key = f"{project_name}:queue"
        self.crawled_key = f"{project_name}:crawled"

        # Boot once, ensure the homepage is added to queue
        if not redis_client.sismember(self.queue_key, self.base_url) and \
           not redis_client.sismember(self.crawled_key, self.base_url):
            redis_client.sadd(self.queue_key, self.base_url)

    def crawl_page(self, thread_name, page_url):
        if redis_client.sismember(self.crawled_key, page_url):
            return

        print(f"[{self.project_name}] {thread_name} is crawling {page_url}")

        links = self.gather_links(page_url)
        self.add_links_to_queue(links)

        redis_client.srem(self.queue_key, page_url)
        redis_client.sadd(self.crawled_key, page_url)

    def gather_links(self, page_url):
        html_string = ''
        try:
            response = requests.get(page_url, timeout=5)
            if 'text/html' in response.headers.get('Content-Type', ''):
                html_string = response.text
            soup = BeautifulSoup(html_string, 'html.parser')
            found_links = set()
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                url = urljoin(page_url, href)
                url = url.split('#')[0]  # remove URL fragments
                if self.is_valid_url(url):
                    found_links.add(url)
            return found_links
        except Exception as e:
            print(f"[{self.project_name}] Error gathering links from {page_url}: {e}")
            return set()

    def add_links_to_queue(self, links):
        for url in links:
            if not redis_client.sismember(self.queue_key, url) and \
               not redis_client.sismember(self.crawled_key, url) and \
               self.domain_name in url and \
               any(d in url for d in self.domain_to_include):
                redis_client.sadd(self.queue_key, url)

    def is_valid_url(self, url):
        parsed = urlparse(url)
        return bool(parsed.netloc) and bool(parsed.scheme)

    @property
    def crawled(self):
        return redis_client.smembers(self.crawled_key)

    @property
    def queue(self):
        return redis_client.smembers(self.queue_key)
