from sqlalchemy import Boolean, Column, Integer, Table, UnicodeText
import knowledge

def get_fact_from_parents(fact, entity):
    """
        Crawl through this entity's parents and return the first
        'fact' it finds.
    """
    while entity.parent:
        if fact in entity.parent.facts:
            return entity.parent[fact]
        entity = entity.parent

def get_magic(filename):
    """ Return the magic type of a filename """
    import magic
    if hasattr(magic, 'from_file'): # python-magic on PyPi
        return magic.from_file(filename)
    else:
        m = magic.open(magic.MAGIC_NONE)
        m.load()
        return m.file(filename)

def get_mapped_table_model_from_entity(entity):
    model = get_polymorphic_model_object()
    table = get_table_from_entity(entity)
    mapper(model, table)
    return table, model

def get_polymorphic_model_object():
    """ Return a model object class that can be mapped to a table """
    class DynamicModelObject(object):
        __civx__ = {'skip_header': True, 'polymorphic': True}
        def __init__(self, **kw):
            for key, value in kw.items():
                setattr(self, key, value)
        def __repr__(self):
            str = ''
            from knowledge.model import Entity, with_characteristic, Fact, DBSession
            table_name = self._sa_class_manager.mapper.mapped_table.name
            # FIXME:
            # (ProgrammingError) CASE types integer and text cannot be matched
            # LINE 5: ...ger' THEN moksha_facts.int_value WHEN 'char' THEN
            #entity = Knowledge.query(Entity).filter(Entity.facts.any(
            #    with_characteristic(u'table_name', table_name))).one()

            entity = DBSession.query(Fact).filter_by(key=u'table_name',
                    char_value=table_name).one().entity
            str += entity.name
            for i, col in enumerate(entity[u'columns']):
                str += '\n * %s: %s' % (entity[u'column_names'][i],
                                        getattr(self, col))
            return str
    return DynamicModelObject

def get_table(table_name):
    """ Return a SQLAlchemy Table """
    from knowledge.model import metadata
    for table in metadata.sorted_tables:
        if table.name == table_name:
            return table

def get_table_from_entity(entity):
    """ Return a SQLAlchemy Table built for a given Entity """
    from knowledge.model import metadata
    table = get_table(entity[u'table_name'])
    if table is not None:
        return table
    cols = [Column('id', Integer, primary_key=True),
            Column('graveyard', UnicodeText),
            Column('flag', Boolean, default=False)]
    for col in entity[u'columns']:
        cols.append(Column(col, UnicodeText))
    return Table(entity[u'table_name'], metadata, *cols)
