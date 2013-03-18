from polyscraper.poly import PolyScraper

url = u'http://www.data.gov/raw/994'
poly = PolyScraper()
poly.consume({'body': {'url': url}})
entity = poly.DBSession.query(Entity).filter_by(name=u'data.gov').one()
len(entity.children)
# 1
child = entity.children.values()[0]
child.name
# u'Department of Commerce'
child = child.children.values()[0]
child.name
# u'Denied Persons List with Denied US Export Privileges'
child[u'repo']
# u'data.gov'
child[u'Agency']
# u'Department of Commerce'
len(child.children)
# 2
child = child.children.values()[1]
assert u'table_name' in child.facts
assert u'column_names' in child.facts
assert u'columns' in child.facts
assert child[u'filename'].endswith('csv')
table, model = get_mapped_table_model_from_entity(child)
int(poly.DBSession.query(model).count())
# 417
poly.DBSession.query(model).first()
# http://data.gov/download/994/csv
#  * Name: A. ROSENTHAL (PTY) LTD.
#  * Street_Address: P.O. BOX 44198, 65 7TH STREET, DENMYR BUILDING
#  * City: LINDEN
#  * State:
#  * Country: ZA
#  * Postal_Code: 2104
#  * Effective_Date: 08/08/1997
#  * Expiration_Date: 08/08/2017
#  * Standard_Order: Y
