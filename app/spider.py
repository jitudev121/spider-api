from bs4 import BeautifulSoup
from app.link_finder import LinkFinder
from app.general import *
from protego import Protego
from urllib.parse import urljoin,urlparse

class Spider:

    project_name = ''
    base_url = ''
    domain_name = ''
    queue_file = ''
    crawled_file = ''
    domain_to_include = ''
    queue = set()
    crawled = set()
    rp = None

    def __init__(self,project_name,base_url,domain_name,domain_to_include):
        Spider.project_name = f'temp/{project_name}'
        Spider.base_url = base_url
        Spider.domain_name = domain_name
        Spider.queue_file = Spider.project_name + '/queue.txt'
        Spider.crawled_file = Spider.project_name + '/crawled.txt'
        Spider.domain_to_include = domain_to_include
        self.boot()
        self.setup_robots()
        self.crawl_page('First Spider',Spider.base_url)
    
    def setup_robots(self):
        robots_url = urljoin(Spider.base_url,'robots.txt')
        get_robots_data = requests.get(robots_url).text
        Spider.rp = Protego.parse(get_robots_data)

    @staticmethod
    def boot():
        create_project_dir(Spider.project_name)
        create_data_files(Spider.project_name,Spider.base_url)
        Spider.queue = file_to_set(Spider.queue_file)
        Spider.crawled = file_to_set(Spider.crawled_file)

    @staticmethod
    def crawl_page(thread_name,page_url):
        if page_url not in Spider.crawled:
            print(thread_name + ' now crawling ' + page_url)
            print('Queue ' + str(len(Spider.queue)) + ' | Crawled ' + str(len(Spider.crawled)))
            Spider.add_links_to_queue(Spider.gather_link(page_url))
            Spider.queue.remove(page_url)
            Spider.crawled.add(page_url)
            Spider.update_files()
    
    @staticmethod
    def gather_link(page_url):
        html_string = ''
        try:
            # print(page_url)
            response = getFileContent(page_url)
            html_bytes = BeautifulSoup(response, 'html.parser')
            html_string  = str( html_bytes )
            finder = LinkFinder(Spider.base_url,page_url)
            finder.feed(html_string)
        except Exception as e:
            print(e)
            return set()
        return finder.page_links()
    
    
    @staticmethod
    def add_links_to_queue(links):
        for url in links:
            parsed_url = urlparse(url)
            if parsed_url.hostname not in Spider.domain_to_include:
                continue
            if url in Spider.queue:
                continue
            if url in Spider.crawled:
                continue
            if Spider.can_fetch_url(url):
                Spider.queue.add(url)

    @staticmethod
    def update_files():
        set_to_file(Spider.queue,Spider.queue_file)
        set_to_file(Spider.crawled,Spider.crawled_file)
        remove_duplicate_url(Spider.crawled_file)


    @staticmethod
    def can_fetch_url(url):
        if '?' in url and not url.endswith('/'):
            base_url = url.split('?')[0]
            query_string = url.split('?')[1]
            normalized_url = f"{base_url}/?{query_string}"
            return Spider.rp.can_fetch(normalized_url, "*")
        return Spider.rp.can_fetch(url, "*")