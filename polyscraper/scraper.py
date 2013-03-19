import urllib

from bs4 import BeautifulSoup

class Scraper(object):
    def get_browser(self):
        """ Get a Twill browser """
        return get_browser()

    def get_magic(self, filename):
        """ Return the magic type of a filename """
        import magic
        if hasattr(magic, 'from_file'): # python-magic on PyPi
            return magic.from_file(filename)
        else:
            m = magic.open(magic.MAGIC_NONE)
            m.load()
            return m.file(filename)

    def get_soup(self, url):
        """ Get a BeautifulSoup object for a given url """
        return BeautifulSoup(urllib.urlopen(url).read())

    def get_engine(self):
        return engine
