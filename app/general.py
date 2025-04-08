import os
import requests
import json
import urllib.robotparser
import pandas as pd
from urllib.request import urlopen
from urllib.request import Request
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import threading

file_write_lock = threading.Lock()


class RoundRobinFetcher:
    def __init__(self):
        self.counter = 0
  
    def get_file_content(self, page_url):
        try:
            if self.counter % 2 == 0:
                page_source = requests.get(page_url)
            else:
                page_source = requests.get(page_url)
            self.counter += 1
            json_page_source = page_source.text
            return json_page_source
        except ValueError:
            print(f"Error: Response from {page_url} is not valid JSON")
            return None
        # return json_page_source['page_source']

fetcher = RoundRobinFetcher()

def create_project_dir(directory):
    if not os.path.exists(directory):
        print('Creating Project' + directory)
        os.makedirs(directory)

def create_data_files(project_name, base_url):
    queue = project_name + '/queue.txt'
    crawled = project_name + '/crawled.txt'
    if not os.path.isfile(queue):
        write_file(queue, base_url)
    if not os.path.isfile(crawled):
        write_file(crawled, '')

def write_file(path, data):
    with open(path, 'w') as f:
        f.write(data)

def append_to_file(path, data):
    with file_write_lock:  # This ensures only one thread writes at a time
        with open(path, 'a') as file:
            file.write(data + '\n')

def delete_file_content(path):
    with open(path, 'w'):
        pass

def file_to_set(file_name):
    result = set()
    with open(file_name, 'rt') as f:
        for line in f:
            result.add(line.replace('\n', ''))
    return result

def set_to_file(links, file):
    delete_file_content(file)
    for link in sorted(links):
        append_to_file(file, link)

def getFileContent(page_url):
    return fetcher.get_file_content(page_url)

def remove_duplicate_url(file):
    with file_write_lock:
        urls = file_to_set(file)
        with open(file, 'w') as f:
            for url in sorted(urls):
                f.write(url + '\n')


