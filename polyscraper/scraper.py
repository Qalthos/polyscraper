import urllib

from bs4 import BeautifulSoup

class Scraper(object):
    git_repo = None

    def get_browser(self):
        """ Get a Twill browser """
        return get_browser()

    def get_engine(self):
        return engine

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

    def download_file(self, url):
        self.log.info("Downloading %s" % url)
        # If a git repo is specified, move the file there.
        if self.git_repo:
            repodir = self.get_repo_dir()
            filename, status = urllib.urlretrieve(url,
                    filename=os.path.join(repodir, url.split('/')[-1]))
        else:
            filename, status = urllib.urlretrieve(url)

        return filename
