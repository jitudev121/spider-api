from bs4 import BeautifulSoup
from app.link_finder import LinkFinder
from app.general import *
from protego import Protego
from urllib.parse import urljoin, urlparse
import requests

class Spider:
    def __init__(self, project_name, base_url, domain_name, domain_to_include):
        self.project_name = f'temp/{project_name}'
        self.base_url = base_url
        self.domain_name = domain_name
        self.queue_file = self.project_name + '/queue.txt'
        self.crawled_file = self.project_name + '/crawled.txt'
        self.domain_to_include = domain_to_include
        self.queue = set()
        self.crawled = set()
        self.rp = None

        self.boot()
        self.setup_robots()
        self.crawl_page('First Spider', self.base_url)

    def setup_robots(self):
        try:
            robots_url = urljoin(self.base_url, 'robots.txt')
            get_robots_data = requests.get(robots_url, timeout=10).text
            self.rp = Protego.parse(get_robots_data)
        except Exception as e:
            print(f"Error loading robots.txt: {e}")
            self.rp = Protego.parse("")  # Empty fallback

    def boot(self):
        create_project_dir(self.project_name)
        create_data_files(self.project_name, self.base_url)
        self.queue = file_to_set(self.queue_file)
        self.crawled = file_to_set(self.crawled_file)

    def crawl_page(self, thread_name, page_url):
        if page_url not in self.crawled:
            print(thread_name + ' now crawling ' + page_url)
            print('Queue ' + str(len(self.queue)) + ' | Crawled ' + str(len(self.crawled)))
            self.add_links_to_queue(self.gather_links(page_url))
            self.queue.discard(page_url)
            self.crawled.add(page_url)
            self.update_files()

    def gather_links(self, page_url):
        html_string = ''
        try:
            response = getFileContent(page_url)
            html_bytes = BeautifulSoup(response, 'html.parser')
            html_string = str(html_bytes)
            finder = LinkFinder(self.base_url, page_url)
            finder.feed(html_string)
        except Exception as e:
            print(f"Error while gathering links from {page_url}: {e}")
            return set()
        return finder.page_links()

    def add_links_to_queue(self, links):
        for url in links:
            if not self.is_valid_url(url):
                continue
            parsed_url = urlparse(url)
            if parsed_url.hostname not in self.domain_to_include:
                continue
            if url in self.queue or url in self.crawled:
                continue
            if self.can_fetch_url(url):
                self.queue.add(url)

    def update_files(self):
        set_to_file(self.queue, self.queue_file)
        set_to_file(self.crawled, self.crawled_file)
        remove_duplicate_url(self.crawled_file)
        remove_duplicate_url(self.queue_file)

    def can_fetch_url(self, url):
        if self.rp:
            if '?' in url and not url.endswith('/'):
                base_url = url.split('?')[0]
                query_string = url.split('?')[1]
                normalized_url = f"{base_url}/?{query_string}"
                return self.rp.can_fetch(normalized_url, "*")
            return self.rp.can_fetch(url, "*")
        return True  # If robots.txt failed, allow by default
    def is_valid_url(self, url):
        try:
            parsed = urlparse(url)
            return all([parsed.scheme in ('http', 'https'), parsed.netloc])
        except Exception:
            return False
