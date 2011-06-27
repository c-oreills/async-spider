#!/usr/bin/python
"""
Asyncronous spider to fetch and process web pages, looking for profanities to
make mailing list moderation easier.
"""

from BeautifulSoup import BeautifulSoup
from urlparse import urldefrag, urljoin, urlparse

import gevent
from gevent import monkey
from gevent.queue import JoinableQueue

monkey.patch_all()

import urllib2

# Optional colourful printing
try:
    from termcolor import cprint
except ImportError:
    def cprint(string, *args):
        print string

PROFANITIES = [
    'bunghole',
    'crap',
    'bowels',
    'hosed',
    'penistone',
    'poop',
    'sofa-king',
    'testes',
    'tool',
    'turd',
    ]

try:
    import django
except ImportError:
    pass
else:
    # Django shipped with a profanities list until 1.3
    if django.VERSION < (1, 3):
        from django.conf.global_settings import PROFANITIES_LIST
        PROFANITIES.extend(PROFANITIES_LIST)

START_URL = 'http://www.erfworld.com/wiki/index.php'
MAX_DEPTH = 1
NO_OF_WORKERS = 10


def job(url, depth_left, queue):
    """
    Fetch a url, process it and add any urls in it to the job queue.
    """
    global seen_urls

    data = fetch_url(url)
    if data is None:
        return

    page_soup = BeautifulSoup(data)
    process_page_soup(page_soup, url)

    if depth_left > 0:
        rel_urls = get_page_links(page_soup)

        for rel_url in rel_urls:
            abs_url = parse_rel_url(rel_url, url)
            if abs_url not in seen_urls:
                seen_urls.add(abs_url)
                queue.put((abs_url, depth_left-1))


def parse_rel_url(rel_url, url):
    """
    Turn a relative url into a normalised absolute url, without a fragment
    """
    abs_url = urljoin(url, rel_url)
    new_url, frag = urldefrag(abs_url)
    #new_url = parsed_url.geturl()
    return new_url

def fetch_url(url):
    """
    Attempt to fetch a url and return the data, with verbose output.
    """
    cprint('Starting %s' % url, 'green')
    try:
        data = urllib2.urlopen(url).read()
    except urllib2.HTTPError as e:
        cprint('Error for %s: %s' % (url, e,), 'red')
        return None
    else:
        cprint('  %s: %s bytes: %r' % (url, len(data), data[:50]), 'cyan')
        return data

def process_page_soup(page_soup, url):
    """
    Process the web page as needed.
    In this example, the page is scanned for profanities.
    """
    # Strip out all tags, leaving only text
    page_text = ''.join(page_soup.findAll(text=True))

    bad_words = [word for word in PROFANITIES if word in page_text.lower()]
    if bad_words:
        cprint('%s found in %s' % (', '.join(bad_words), url), 'white', 'on_red')
        # Extension: Add this to results list to email to moderator

def get_page_links(page_soup):
    """
    Return all urls on a page if they haven't already been seen.
    """
    rel_urls = []
    for link in page_soup.findAll('a'):
        attrs = dict(link.attrs)
        if u'href' in attrs:
            href = attrs[u'href'] 
            rel_urls.append(href)
    return rel_urls

def job_worker(queue):
    while True:
        url, depth_left = queue.get()
        try:
            job(url, depth_left, queue)
        finally:
            queue.task_done()
  
seen_urls = set((START_URL,))
job_queue = JoinableQueue()
job_queue.put((START_URL, MAX_DEPTH))

for i in range(NO_OF_WORKERS):
    gevent.spawn(job_worker, job_queue)

job_queue.join()

