# -*- coding: utf-8 -*-
# This file is part of CIVX.
#
# CIVX is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# CIVX is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with CIVX.  If not, see <http://www.gnu.org/licenses/>.
#
# Copyright 2008-2010, CIVX, Inc.
"""
The CIVX Polymorphic Scraper
============================

How it works
------------

 * URL is provided through an AMQP message sent to the `civx.scrapers.new_url`
   topic, or via the PolyForm widget (which hits the MessageController).
 * The PolyScraper, which is a Moksha Consumer that runs in The Hub, consumes
   the message
 * If there is a URL Handler that matches the specified URL, it is executed,
   and responsible for manipulating the page to find/download data
 * Interesting data is scraped from the page
 * The 'magic pattern' of the data is inspected, and handled appropriately
 * Data is committed to git repository
 * New lines of data are extracted with git, and passed to the appropriate
   populator methods
 * The data is scrubbed and populated in it's own database table on the fly
 * Message is sent to broker with details once it is complete
 * Datasets can be viewed in the PolyGrid (via /widgets/polygrid?model=<filename>)

TODO:
 * Make sure it only populates the *new* data from git!!
    we've got duplicate tables getting created now

 * Datasets profiles are created
    - dataset is self documenting
 * Data is visualized in grids
    - limit hide cols for huge datasets
    - eventually enable persistent grid configuration
        either via session cookie in jqgrid, or with a WidgetConfiguration model
 * Data is exposed via JSON API
 * Data is avaiable via CIVX Python API or command-line client


Running the PolyScraper tests
-----------------------------

 $ paver test_polyscraper

.. moduleauthor:: Luke Macken <luke@civx.us>
.. moduleauthor:: Nathaniel Case <geopirate@civx.us>
.. moduleauthor:: RemyD <remyd@civx.us>
"""

import re
import os
import csv
import shutil
import logging
import uuid
import subprocess

from twill.commands import fv, save_html, submit
from datetime import datetime
from urlparse import urlparse, urljoin
from pprint import pprint, pformat
from sqlalchemy import *
from sqlalchemy.orm import mapper, sessionmaker
from knowledge.model import Fact, Entity, DBSession

from bs4 import BeautifulSoup

from polyscraper import utils
from polyscraper.scraper import Scraper

extensions = u'csv,zip,exe,xls,txt,rss,xml,json'

class PolyScraper(Scraper):
    """ The CIVX Polymorphic Scraper. """

    def __init__(self):
        self.log = logging.getLogger('PolyScraper')
        self.config = {'git_dir': '/tmp/git'}

    def consume(self, url):
        """
        This method attempts to scrape a URI.  First it tries to figure out the
        protocol, then tries to pull a hostname out of the url.  Then the git
        repo is initialized, and we take a close look at the url.

        If the hostnme is known to be tricky, it will have a special handler
        method written for it and leave from there.  Otherwise it goes through
        the general path for its protocol, attempting to find useful data.

        When everything is done, the entity is updated and messages are sent
        out announcing that the scrape is done.
        """
        self.log.debug("PolyScraper(%s)" % url)
        start = datetime.utcnow()

        # Try to pull a protocol off the URI
        protocol_end = url.find("://")
        protocol = "http"
        if not protocol_end == -1:
            protocol = url[:protocol_end]

        parsed_url = urlparse(url)
        hostname = parsed_url[1].replace('www.', '')
        # Set a hostname if none is set.
        if not hostname:
            hostname = u"localhost"

        # See if we already know about this URL
        entity = Entity.by_name(url)
        if entity:
            self.log.info('Entity(%r) already exists' % url)
        else:
            root = Entity.by_name(u'CIVX')
            if not root:
                root = Entity(name=u'CIVX')
                DBSession.add(root)
                DBSession.flush()

            parent = Entity.by_name(hostname)
            if not parent:
                parent = Entity(name=hostname)
                DBSession.add(parent)
                parent.parent = root
                self.log.debug("Created entity %r" % parent.name)

            entity = Entity(name=url)
            DBSession.add(entity)
            # hide the exact url entity from our tree
            entity.parent = parent
            self.log.debug("Created entity %r" % entity.name)

            #self.send_message('civx.knowledge.entities.new', {
            #    'msg': 'New entity created: %s' % url
            #    })

        DBSession.flush()

        # Initialize a git repo for this data source
        entity[u'repo'] = hostname
        #entity[u'url'] = url

        # Initialize the git repository for this domain
        #~ self.init_git_repo(repo=hostname)
        DBSession.flush()

        # Scrape the url (to a certain depth) for data
        num_downloads = 0

        # Provide a URL handler method that is called with each file pass
        # in the soup entity for the link instead, so we can easily look
        # around the DOM and pull out titles, etc.
        if hostname in self.url_handlers:
            #self.url_handlers[hostname](self, soup_link, file_entity)
            self.url_handlers[hostname](self, url)
        else:
            # If we do not specifically handle this file, take a basic approach
            # based on the protocol.  These could probably also be split off
            # into $protocol_handler methods.
            self.log.warning('Cannot find %s URL handler' % hostname)
            files = []
            if protocol == "ftp":
                from ftplib import FTP
                self.log.debug("FTP support is not implemented yet.")
            elif protocol == "file":
                search_path = url[protocol_end+3:]
                local_files = []
                if os.path.isdir(search_path):
                    # Find all files in directory
                    for directory in os.walk(search_path):
                        dirpath = directory[0]
                        for filename in directory[2]:
                            local_files.append(os.path.join(dirpath, filename))
                else:
                    local_files.append(search_path)

                dest = os.path.join(self.config['git_dir'], hostname)

                # FIXME: what about for links to epa.gov from data.gov?
                # we probably want our own epa.gov repo namespace to download
                # and extract this to
                #if not os.path.isdir(dest):
                #    self.log.debug("mkdir %s" % dest)
                #    os.makedirs(dest)

                # I think this section is deprecated and unnecessary...
                #for ext in extensions.split(','):
                ##    if link.endswith('%s' % ext) or '/%s/' % ext in link:
                ##        entity[u'format'] = ext
                #    if ext not in civx.model.models[Entity]:
                #        civx.model.models[Entity][ext] = []

                for path in local_files:
                    #raw = self.download_file(link)
                    #file_name = os.path.basename(link)
                    #filename = to_unicode(os.path.join(dest, file_name))
                    #num_downloads += 1

                    #shutil.copy(raw, filename)
                    #self.log.debug("Copied %s to %s" % (raw,
                    #    os.path.join(dest, file_name)))

                    ##file_entity = Entity(name=os.path.basename(file_name))
                    #file_entity = Entity(name=link)
                    ##file_entity[u'url'] = link
                    #file_entity[u'filename'] = filename
                    #file_entity[u'repo'] = hostname
                    #DBSession.add(file_entity)
                    #file_entity.parent = entity
                    ##file_entity.parent = parent
                    #self.log.debug("Created entity %r (parent %r)" % (
                    #    file_entity.name, file_entity.parent.name))

                    file_path = os.path.split(path)[0]
                    file_name = os.path.split(path)[1]
                    self.log.info("%s is a local file" % file_name)
                    files.append((file_path, file_name, path))
                    #files.append((os.path.dirname(filename), file_name, filename))

            else:
                # Assume protocol is http
                """
####
            f = urllib2.urlopen(url) # XXX: does this load everything into mem?
            if f.info().type == 'text/html':
                soup = self.get_soup(f.read())
            else: # Assume the url is a link to a direct file
                # Save the file to disk.
                # throw file at magic handlers
####
"""
                soup = self.get_soup(url)

                for link, soup_link in self.scrape_files_from_url(url, soup_links=True):
                    parsed_link = urlparse(link)
                    file_path = '/'.join(parsed_link[2].split('/')[:-1])
                    file_name = parsed_link[2].split('/')[-1]
                    files.append((file_path, file_name, link))

            for (file_path, file_name, link) in files:
                dest = self.config['git_dir'] + hostname + file_path
                local = os.path.exists(link)

                # See if this file already exists
                file_entity = Entity.by_name(link)
                #file_entity = Entity.by_name(os.path.basename(file_name))
                if file_entity:
                    self.log.info('Entity(%r) already exists; skipping.' % link)
                    continue

                # FIXME: what about for links to epa.gov from data.gov?
                # we probably want our own epa.gov repo namespace to download
                # and extract this to
                if not os.path.isdir(dest):
                    os.makedirs(dest)

                # I think this section is deprecated and unnecessary...
                for ext in extensions.split(','):
                #    if link.endswith('%s' % ext) or '/%s/' % ext in link:
                #        entity[u'format'] = ext
                    if ext not in civx.model.models[Entity]:
                        civx.model.models[Entity][ext] = []

                raw = self.download_file(link)
                filename = os.path.join(dest, file_name)
                num_downloads += 1

                if local:
                    self.log.debug("Copied %s to %s" % (raw, filename))
                    shutil.copy(raw, filename)
                else:
                    self.log.debug("Moved %s to %s" % (raw, filename))
                    shutil.move(raw, filename)

                #file_entity = Entity(name=os.path.basename(file_name))
                file_entity = Entity(name=link)
                #file_entity[u'url'] = link
                file_entity[u'filename'] = filename
                file_entity[u'repo'] = hostname
                DBSession.add(file_entity)
                file_entity.parent = entity
                #file_entity.parent = parent
                self.log.debug("Created entity %r (parent %r)" % (
                    file_entity.name, file_entity.parent.name))

                # Determine the file magic, and call the appropriate handler
                file_entity[u'magic'] = self.call_magic_handler(filename, file_entity)
                DBSession.flush()

# To do this stuff we'll need to return an entity from the url handler?
        #if 'num_files' in entity.facts:
        #    num_files = int(entity['num_files'])
        #    print repr(num_files)
#
#            if num_files != num_downloads:
#                self.log.info('Downloaded %d more files from previous scrape' %
#                              num_downloads - num_files)
#            entity[u'num_files'] += num_downloads
#        else:
        #entity[u'num_files'] = num_downloads
        #if u'date_added' not in entity.facts:
        #    entity[u'date_added'] = unicode(datetime.utcnow())
        #entity[u'date_last_scraped'] = unicode(datetime.utcnow())

        if 'changelog' not in entity.facts:
            entity[u'changelog'] = []

        finish = datetime.utcnow()

        changelog = {
            u'start_time': unicode(start),
            u'finish_time': unicode(finish),
            u'elapsed_time': unicode(finish-start),
            u'num_downloads': num_downloads,
            #u'num_children': len(entity.children),
            #~ u'git_commit': self.get_latest_commit_id(),
            }
        entity[u'changelog'].append(changelog)

        DBSession.commit()

        self.log.info("== Statistics ==")
        self.log.info("Scraped url: " + url)
        self.log.info("Number of downloaded files: %d" % num_downloads)

        #self.send_message('civx.knowledge.entitites.%s' % url, {
        #    'msg': 'Completed scraping %s' % url,
        #    'changelog': changelog,
        #    })

    # File type handlers
    def zip_exe_handler(self, entity):
        """ Handles self-extracting zip files """
        self.log.debug("zip_exe_handler(%s)" % entity)
        entity[u'format'] = u'zip'
        dirname = os.path.dirname(entity[u'filename'])
        p = subprocess.Popen('unzip -o "%s"' % entity[u'filename'],
                             shell=True, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             cwd=dirname)
        out, err = p.communicate()
        if err:
            self.log.error("Error unzipping: " + err)
        else:
            # Delete compressed data after extracting
            os.unlink(entity[u'filename'])

        for line in out.split('\n'):
            if line.strip().startswith('inflating'):
                extracted = os.path.join(dirname, line.strip().split()[-1])
                self.log.debug("extracted " + extracted)
                magic = self.get_magic(extracted)

                # Create a new child Entity for each extracted file
                extracted = to_unicode(extracted)
                child = Entity.by_name(extracted)
                if not child:
                    child = Entity(name=os.path.basename(extracted))
                    child[u'filename'] = extracted
                    DBSession.add(child)
                    child.parent = entity
                    child[u'magic'] = to_unicode(magic)
                    self.log.debug("Created %s" % child)
                else:
                    child.parent = entity

                DBSession.flush()

                self.call_magic_handler(extracted, child)

    def tgz_handler(self, entity):
        """ Handles .tgz files """
        self.log.debug("tgz_handler(%s)" % entity)
        entity[u'format'] = u'tgz'
        dirname = os.path.dirname(entity[u'filename'])
        p = subprocess.Popen('tar -zxvf "%s"' % entity[u'filename'],
                             shell=True, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             cwd=dirname)
        out, err = p.communicate()
        if err:
            self.log.error("Error unzipping: " + err)
        else:
            # Delete compressed data after extracting
            os.unlink(entity[u'filename'])

        for line in out.split('\n'):
            if line.strip().startswith('inflating'):
                extracted = os.path.join(dirname, line.strip().split()[-1])
                self.log.debug("extracted " + extracted)
                magic = self.get_magic(extracted)

                # Create a new child Entity for each extracted file
                child = Entity.by_name(extracted)
                if not child:
                    child = Entity(name=os.path.basename(extracted))
                    child[u'filename'] = extracted
                    DBSession.add(child)
                    child.parent = entity
                    child[u'magic'] = to_unicode(magic)
                    self.log.debug("Created %s" % child)
                else:
                    child.parent = entity

                DBSession.flush()

                self.call_magic_handler(extracted, child)

    def ascii_text_handler(self, entity):
        self.log.debug("ascii_text_handler(%s)" % entity)
        link = self.get_fact_from_parents(u'link', entity)
        repo = self.get_fact_from_parents(u'repo', entity)
        #~ self.git_add_and_commit(entity.name, repo=repo)
        self.polymorphic_csv_populator(entity)

    def polymorphic_csv_populator(self, entity):
        try:
            #flush_after = asint(config.get('transaction_size', 1000))
            repo = self.get_fact_from_parents(u'repo', entity)
            custom_dialect = self.dialects.get(repo, None)
            if not custom_dialect:
                # Nothing to see hre, carry on.
                pass
            elif custom_dialect not in csv.list_dialects():
                self.log.error("Dialect '%s' not found!" % custom_dialect)
            scrubber = CSVScrubber(entity[u'filename'], dialect=custom_dialect)
            columns = None
            # TODO: See if this file has already been parsed!

            for i, line in enumerate(scrubber.readlines()):
                if i == 0:
                    columns = line

                    # then create a Table object with the appropriate columns
                    table_name = u'civx_' + unicode(uuid.uuid4()).replace('-', '')
                    entity[u'table_name'] = table_name
                    entity[u'column_names'] = columns
                    # The actual column names behind the scenes.  CIVX will
                    # map them to the 'column_names'
                    entity[u'columns'] = [u'col_%d' % i for i in
                                          range(len(columns))]
                    #DBSession.flush()
                    table, model = get_mapped_table_model_from_entity(entity)
                    model.__table__ = table

                    civx.model.models[model] = {
                            'csv': [entity[u'filename']],
                            'columns': entity[u'columns'],
                            'tmp_csv': {}
                            }

                    metadata.create_all()
                    continue
                break

            self.log.info("%d entries in %r table" % (
                    DBSession.query(model).count(),
                    entity[u'table_name']))

            populate_csv((
                    self.get_fact_from_parents('repo', entity),
                    entity[u'filename'],
                    model,
                    self.engine), dialect=custom_dialect)

            self.log.info("%d entries in %r table" % (
                    DBSession.query(model).count(),
                    entity[u'table_name']))

            DBSession.commit()

        except Exception, e:
            self.log.error('Unable to parse file as CSV')
            self.log.exception(e)

    # Hostname specific handlers
    def data_gov_handler(self, url):
        """ data.gov handler.

        Entity(CIVX)
        |-Entity(data.gov)
        | |-Entity(Agency)
        |    |-Entity(http://www.data.gov/raw/674)
        |    | |-Fact(title), ...
        |    | |-Entity(http://www.epa.gov/tri/tridata/tri08/early_data/statedata/basic/TRI_2008_TN_v08.exe)

        """
        self.log.debug('data_gov_handler(%s)' % locals())
        parsed_url = urlparse(url)
        hostname = parsed_url[1].replace('www.', '')
        data_types = ('csv', 'RDF', 'xml', 'kml', 'PDF', 'shapefile', 'XLS')
        fields = ('Agency', 'Sub-Agency', 'Category', 'Date Released',
                  'Date Updated', 'Time Period', 'Frequency',
                  'Description', 'Data.gov Data Category Type',
                  'Specialized Data Category Designation',
                  'Keywords', 'Unique ID', 'Citation',
                  'Agency Program Page', 'Agency Data Series Page',
                  'Unit of Analysis', 'Granularity', 'Geographic Coverage',
                  'Collection Mode', 'Data Collection Instrument',
                  'Data Dictionary/Variable List', 'Technical Documentation',
                  'Additional Metadata')

        # Our top-level data.gov entity
        data_gov = Entity.by_name('data.gov')
        if not data_gov:
            data_gov = Entity(name=u'data.gov')
            DBSession.add(data_gov)
            root = Entity.by_name(u'CIVX')
            if not root:
                root = Entity(name=u'CIVX')
                DBSession.add(root)
            data_gov.parent = root

        # See if this entity already exists
        #~ entity = Entity.by_name(url)
        #~ if entity:
            #~ self.log.info('Entity(%r) already exists; skipping.' % url)
            #~ return

        soup = self.get_soup(url)

        # If this is a raw data profile, grab the title of the dataset
        if '/raw/' in url:
            # Create a new Entity for this URL
            title = soup.find('h2', {'id': 'datasetName'}).string.decode('utf-8', 'replace')
            entity = Entity(name=title)
            entity[u'url'] = url
            entity[u'repo'] = hostname
            DBSession.add(entity)
            dest = [self.config['git_dir'], hostname]

            # Extract data for each field
            for field in fields:
                data = soup.find(text=field)
                if data and data.next and data.next.next:
                    data = data.next.next.string
                    if data:
                        entity[unicode(field)] = data.decode('utf-8').strip()

            DBSession.flush()

            # Create seperate parent Agency Entity
            if u'Agency' in entity.facts:
                agency = Entity(name=entity[u'Agency'])
                agency.parent = data_gov
                parent = agency
                DBSession.add(agency)
                dest.append(entity[u'Agency'])
                if u'Sub-Agency' in entity.facts:
                    subagency = Entity(name=entity[u'Sub-Agency'])
                    subagency.parent = agency
                    parent = subagency
                    DBSession.add(subagency)
                    dest.append(entity[u'Sub-Agency'])
                DBSession.flush()

            # Have the URL be the child of the agency or sub-agency
            entity.parent = parent

            # Elegant repo paths: data.gov/Agency[/Sub-Agency]/title/filename
            dest.append(entity.name)
            dest = os.path.join(*dest)
            if not os.path.isdir(dest):
                os.makedirs(dest)

            # Scrape all available raw data types
            downloads = soup.find_all('a', href=re.compile(r'^/download'))
            for button in downloads:
                data = button.string.split()[0]
                link = button['href']
                if link:
                    link = urljoin('http://explore.data.gov', link)
                    entity[data.lower()] = link
                    parsed_link = urlparse(link)
                    file_name = parsed_link[2].split('/')[-1]

                    raw = self.download_file(link)
                    filename = os.path.join(dest, file_name)
                    shutil.move(raw, filename)
                    self.log.debug("Moved %s to %s" % (raw, filename))

                    # Create a new entity for this file
                    file_entity = Entity(name=link)
                    DBSession.add(file_entity)
                    file_entity[u'filename'] = filename
                    file_entity.parent = entity

                    # Process this file accordingly
                    self.call_magic_handler(filename, file_entity)

            # Find external map links
            map = soup.find('a', href=re.compile(r'^/externallink/map/'))
            if map:
                map = urllib.unquote(map.get('href', '')[18:]).split('/')[0].replace('###', '/')
                entity[u'map'] = map

            DBSession.flush()

        # If this is from a table of results, grab the title fom this row
        else:
            self.log.debug("entity[url] = %r" % entity[u'url'])
            raise NotImplementedError("Scraping titles from data.gov tables not yet supported")

    def opensecrets_handler(self, url):
        """
        This takes care of all the opensecrets.org-specific stuff before going
        to the generalized twill handler
        """

        self.log.debug('opensecrets_handler(%s)' % locals())

        b = self.get_browser()

        def login():
            """
            Tries to log in to the site, does nothing if already logged in.
            """
            b.go(url)
            try:
                fv('2', 'email', 'civx@civx.us')
                fv('2', 'password', 'civxat0s')
                submit('0')
            except:
                # Inelegant, but if already logged in, this will fail as
                # there's no login form.
                pass

        login()
        b.follow_link(b.find_link('Bulk Data'))
        soup = BeautifulSoup(b.get_html())

        links = {}

        # Not all h2s are important, but we want them all anyway.
        titles = soup.findAll('h2')
        for title in titles:
            try:
                if not title.nextSibling.nextSibling.name == 'ul':
                    # But if the tag after the h2 isn't an ul, just skip it.
                    continue
            except AttributeError:
                continue
            # We want all the *connected* li tags, so we need to do this.
            first = title.findNext('li')
            rest = first.findNextSiblings('li')
            rest.insert(0, first)

            try:
                title = title.string.decode('utf-8', 'replace')
            except AttributeError:
                continue
            links[title] = []

            for link in rest:
                links[title].append(link.a)

        self.twill_handler(b.get_url(), links, login)

    # Process specific handlers
    def twill_handler(self, url, links, login_func):
        """
        This function uses twill to download the files defined in links
        and passing them to the magic handler to be processed.
        """

        self.log.debug('twill_handler(%s)' % locals())

        parsed_url = urlparse(url)
        hostname = parsed_url[1].replace('www.', '')

        parent = Entity.by_name(hostname)
        if not parent:
            parent = Entity(name=hostname)
            DBSession.add(parent)
            root = Entity.by_name(u'CIVX')
            if not root:
                root = Entity(name=u'CIVX')
                DBSession.add(root)
            parent.parent = root

        # See if this entity already exists
        entity = Entity.by_name(url)
        if entity:
            self.log.info('Entity(%r) already exists; skipping.' % url)
            return

        #DBSession.flush()
        for category, link_list in links.items():

            dest = [self.config['git_dir'], hostname]

            if len(links) == 1:
                entity = parent
            else:
                entity = Entity(name=category)
                entity[u'url'] = url
                entity[u'repo'] = hostname
                entity.parent = parent
                dest.append(entity.name)

                DBSession.add(entity)
                DBSession.flush()

            dest = os.path.join(*dest)
            if not os.path.isdir(dest):
                os.makedirs(dest)

            b = self.get_browser()
            for link in link_list:
                # We might have timed out, try to log in again.
                login_func()
                b.go(link['href'])
                # Try to pick out the filename if there is a query
                if link['href'].find('=') >= 0:
                    filename = urlparse(link['href'])[-2].split('=')[1]
                else:
                    filename = link['href'].split('/')[-1]
                filename = os.path.join(dest, filename)
                save_html(filename)

                file_entity = Entity(name=link.contents[0])
                file_entity[u'filename'] = filename
                file_entity[u'repo'] = hostname
                DBSession.add(file_entity)
                file_entity.parent = entity
                self.log.debug("Created entity %r (parent %r)" % (
                    file_entity.name, file_entity.parent.name))

                magic = self.call_magic_handler(filename, file_entity)
                file_entity[u'magic'] = magic

                DBSession.flush()

    def call_magic_handler(self, filename, entity):
        """ Determine the file magic, and call the appropriate handler """
        magic = self.get_magic(filename)
        if magic in self.magic_types:
            self.log.info('Calling %r for %s magic' % (
                self.magic_types[magic], magic))
            self.magic_types[magic](self, entity)
        else:
            # Try to match any regex magic patterns
            for pattern in self.magic_types:
                if isinstance(pattern, type(re.compile(r'foo'))):
                    if re.match(pattern, magic):
                        self.log.info('Calling %r for %s magic' % (
                            self.magic_types[pattern], magic))
                        self.magic_types[pattern](self, entity)
                        break
            else:
                self.log.error('No handler for magic: %s' % magic)
        return magic

    ##
    ## A mapping of a file's magic pattern to the appropriate handler for
    ## that specific data type.
    ##
    magic_types = {
        re.compile(r'PE32 executable.*for MS Windows.*'): zip_exe_handler,
        re.compile(r'Zip.*'): zip_exe_handler,
        re.compile(r'ASCII.*'): ascii_text_handler,
        re.compile(r'Non-ISO extended-ASCII.*'): ascii_text_handler,
        re.compile(r'UTF-8.*'): ascii_text_handler,
        re.compile(r'gzip compressed data.*'): tgz_handler,
    }

    url_handlers = {
        'data.gov': data_gov_handler,
        'opensecrets.org': opensecrets_handler,
    }

    dialects = {
        'opensecrets.org': 'opensecrets'
    }
