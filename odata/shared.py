from sqlalchemy.sql.expression import Join


def select_star(sqlobj):
    for col in sqlobj.froms[-1]._from_objects[-1].columns:
        if not col.info.get('hidden'):
            sqlobj = sqlobj.column(col)

    return sqlobj
