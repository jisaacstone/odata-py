from sqlalchemy import Table
from sqlalchemy.sql.expression import Join, Select


def select_star(sqlobj, columns=None):
    if columns is None:
        columns = sqlobj.froms[-1]._from_objects[-1].columns

    for col in columns:
        if not col.info.get('hidden'):
            sqlobj = sqlobj.column(col)

    return sqlobj


def get_tables(sqlobj):
    if hasattr(sqlobj, 'table'):
        table = sqlobj.table
        return {table.info.get('alias', table.name): table}
    if not isinstance(sqlobj, Select):
        raise AttributeError('unexpected sqlobj: ' + sqlobj)
    tables = {}
    for _from in sqlobj.locate_all_froms():
        if isinstance(_from, Table):
            tables[_from.info.get('alias', _from.name)] = _from

    return tables
