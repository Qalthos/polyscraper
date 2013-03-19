import urllib

from bs4 import BeautifulSoup

class Scraper(object):
    def get_browser(self):
        """ Get a Twill browser """
        return get_browser()

    def get_soup(self, url):
        """ Get a BeautifulSoup object for a given url """
        return BeautifulSoup(urllib.urlopen(url).read())

    def get_engine(self):
        return engine
