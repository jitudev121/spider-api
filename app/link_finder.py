from html.parser import HTMLParser
from urllib import parse

class LinkFinder(HTMLParser):

    def __init__(self,base_url,page_url):
        super().__init__()
        self.base_url = base_url
        self.page_url = page_url
        self.links = set()
    
    def handle_starttag(self, tag, attrs):
        if tag=='a':
            for (attribute,value) in attrs:
                if '#' in value:
                    continue
                if attribute == 'href':
                    if not value or value.startswith('#') or value.startswith('javascript:') or value.startswith('mailto:'):
                        continue

                    url = parse.urljoin(self.base_url,value)
                    parsed = parse.urlparse(url)
                    if parsed.scheme in ['http', 'https'] and parsed.netloc:
                        self.links.add(url)
    
    def page_links(self):
        return self.links

    def error(self,message):
        pass

