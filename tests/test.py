from __future__ import print_function, unicode_literals
import unittest

from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from knowledge.model import init_model, metadata, DBSession, Entity

from nose.tools import eq_

from polyscraper.poly import PolyScraper


def dummy_get_soup(self, url):
    return BeautifulSoup(open('/tmp/dpl.html').read())
def dummy_get_file(self, url):
    url_match = {
        'http://explore.data.gov/download/5kvc-rp2e/CSV': '/tmp/git/data.gov/Department of Commerce/Bureau of Industry and Security/Denied Persons List with Denied US Export Privileges/CSV',
        'http://explore.data.gov/download/5kvc-rp2e/RDF': '/tmp/git/data.gov/Department of Commerce/Bureau of Industry and Security/Denied Persons List with Denied US Export Privileges/RDF',
    }
    return url_match[url]
PolyScraper.get_soup = dummy_get_soup
PolyScraper.download_file = dummy_get_file


class test_data_gov_handler(unittest.TestCase):
    def setUp(self):
        engine = create_engine('sqlite:///:memory:')
        init_model(engine)
        metadata.create_all(engine)
        PolyScraper().consume('http://www.data.gov/raw/994')
        self.entity = Entity.by_name('data.gov')

    def tearDown(self):
        DBSession.remove()

    def test_data_gov_size(self):
        eq_(len(self.entity.children), 2)

    def test_agency_name(self):
        child = self.entity.children.values()[1]
        eq_(child.name, 'Department of Commerce')

    def test_subagency_name(self):
        child = self.entity.children.values()[1]
        child = child.children.values()[0]
        eq_(child.name, 'Bureau of Industry and Security')

    def test_document_name(self):
        child = self.entity.children.values()[1]
        child = child.children.values()[0]
        child = child.children.values()[0]
        eq_(child.name, 'Denied Persons List with Denied US Export Privileges')

    def test_document_repo_name(self):
        child = self.entity.children.values()[1]
        child = child.children.values()[0]
        child = child.children.values()[0]
        eq_(child['repo'], 'data.gov')

    def test_document_agency_name(self):
        child = self.entity.children.values()[1]
        child = child.children.values()[0]
        child = child.children.values()[0]
        eq_(child['Agency'], 'Department of Commerce')

    def test_document_children_size(self):
        child = self.entity.children.values()[1]
        child = child.children.values()[0]
        child = child.children.values()[0]
        eq_(len(child.children), 2)

    def test_table_name(self):
        child = self.entity.children.values()[1]
        child = child.children.values()[0]
        child = child.children.values()[0]
        child = child.children.values()[1]
        assert 'table_name' in child.facts

    def test_column_names(self):
        child = self.entity.children.values()[1]
        child = child.children.values()[0]
        child = child.children.values()[0]
        child = child.children.values()[1]
        assert 'column_names' in child.facts

    def test_columns(self):
        child = self.entity.children.values()[1]
        child = child.children.values()[0]
        child = child.children.values()[0]
        child = child.children.values()[1]
        assert 'columns' in child.facts

    def test_filename(self):
        child = self.entity.children.values()[1]
        child = child.children.values()[0]
        child = child.children.values()[0]
        child = child.children.values()[1]
        assert child['filename'].endswith('csv')

    def test_model_count(self):
        child = self.entity.children.values()[1]
        child = child.children.values()[0]
        child = child.children.values()[0]
        child = child.children.values()[1]
        table, model = get_mapped_table_model_from_entity(child)
        eq_(int(DBSession.query(model).count()), 417)

    def test_model_first_row(self):
        child = self.entity.children.values()[1]
        child = child.children.values()[0]
        child = child.children.values()[0]
        child = child.children.values()[1]
        table, model = get_mapped_table_model_from_entity(child)
        eq_(DBSession.query(model).first(), """http://data.gov/download/994/csv
* Name: A. ROSENTHAL (PTY) LTD.
* Street_Address: P.O. BOX 44198, 65 7TH STREET, DENMYR BUILDING
* City: LINDEN
* State:
* Country: ZA
* Postal_Code: 2104
* Effective_Date: 08/08/1997
* Expiration_Date: 08/08/2017
* Standard_Order: Y""")
