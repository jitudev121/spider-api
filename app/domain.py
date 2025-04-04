from urllib.parse import urlparse

def get_domain_name(url):
    try:
        result = get_subdomain_name(url).split('.')
        return result[-2] + '.' + result[-1]
    except:
        return ''


def get_domain_name_url(url):
    try:
        result = urlparse(url).hostname
        return result
    except:
        return ''

def get_subdomain_name(url):
    try:
        return urlparse(url).netloc
    except:
        return ''
