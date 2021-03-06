#!/usr/bin/python
"""
Asyncronous spider to fetch and process web pages, looking for profanities to
make mailing list moderation easier.
"""

from gevent import monkey
monkey.patch_all()

import gevent
from gevent.queue import JoinableQueue

from BeautifulSoup import BeautifulSoup
from urlparse import urldefrag, urljoin, urlparse
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


def fetch_and_process_url(url, depth_left, seen_urls, page_fn):
    """
    Fetch a url, process it and return any urls in it.
    """
    data = fetch_url(url)
    if data is None:
        return

    page_soup = BeautifulSoup(data)
    page_fn(page_soup, url)

    if depth_left > 0:
        unseen_urls = get_unseen_urls_from_page(page_soup, url, seen_urls)
        return unseen_urls

def job(url, depth_left, queue, seen_urls, page_fn):
    """
    Handle fetching and processing of given url and add results to job queue.
    """
    unseen_urls = fetch_and_process_url(url, depth_left, seen_urls, page_fn)
    if unseen_urls is not None:
        for url in unseen_urls:
            queue.put((url, depth_left-1))

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

def get_unseen_urls_from_page(page_soup, url, seen_urls):
    """
    Given page_soup, finds all urls in it and returns any that have not been
    seen before. Returned urls are absolute.
    """
    rel_urls = get_page_links(page_soup)
    unseen_urls = []

    for rel_url in rel_urls:
        abs_url = parse_rel_url(rel_url, url)
        if abs_url not in seen_urls:
            seen_urls.add(abs_url)
            unseen_urls.append(abs_url)
    return unseen_urls

def get_page_links(page_soup):
    """
    Return all urls on a page.
    """
    rel_urls = []
    for link in page_soup.findAll('a'):
        attrs = dict(link.attrs)
        if u'href' in attrs:
            href = attrs[u'href'] 
            rel_urls.append(href)
    return rel_urls

def job_worker(queue, seen_urls, page_fn):
    """
    Loop forever, taking jobs from the queue and executing them.
    """
    while True:
        url, depth_left = queue.get()
        try:
            job(url, depth_left, queue, seen_urls, page_fn)
        finally:
            queue.task_done()
  
def check_page_for_profanities(page_soup, url):
    """
    In this example, the page is scanned for profanities.
    """
    # Strip out all tags, leaving only text
    page_text = ''.join(page_soup.findAll(text=True))

    bad_words = [word for word in PROFANITIES if word in page_text.lower()]
    if bad_words:
        cprint('%s found in %s' % (', '.join(bad_words), url), 'white', 'on_red')
        # Extension: Add this to results list to email to moderator

def spider(start_url, max_depth=1, no_of_workers=10, page_fn=check_page_for_profanities):
    """
    Concurrently spider the web, starting from web page, executing page_fn
    on each page.

    start_url specifies the document the spider starts from.
    max_depth specifies the maximum link depth from the start_url that
    processing will occur.
    no_of_workers specifies how many concurrent workers process the job queue.
    page_fn is a function that takes BeautifulSoup parsed html and a url and
    processes them as required
    """
    seen_urls = set((start_url,))
    job_queue = JoinableQueue()
    job_queue.put((start_url, max_depth))

    for i in range(no_of_workers):
        gevent.spawn(job_worker, job_queue, seen_urls, page_fn)

    job_queue.join()


if __name__ == '__main__':
    spider(START_URL)
