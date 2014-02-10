# encoding: utf-8
import parsley
import functools
from sqlalchemy import exc
from sqlalchemy.sql import expression

from odata.exc import RequestParseError
from odata.exc import NotFoundException


url_grammar = '''
name = <(letter | ' ' | '_')+>:n -> n.lower().replace(' ', '_')
parameter = ','? ws (~(',' | ')') anything)+:txt -> ''.join(txt)
call = '(' parameter*:p ')' -> p
item = name:n (call:c -> callable(n, c)
              | -> item(n) )
count = '$count' -> count()
value = '$value' -> value()
links = '$links/' name:n -> links(n)
segment = item | count | value | links
path = '/'? segment ('/' segment)*
'''


def add_table(sqlobj, table):
    try:
        for tbl in reversed(sqlobj.froms):
            try:
                return sqlobj.select_from(tbl.join(table))
            except (exc.NoForeignKeysError,
                    exc.AmbiguousForeignKeysError):
                continue
    except AttributeError:
        # No tables - need to instantiate
        return sqlobj(table)

    raise RequestParseError('Could not find relationship for {}'
                            .format(table.name))


def parse_pk(table, *keys):
    # TODO: handle join case
    where = None
    if len(table.primary_key) != len(keys):
        raise RequestParseError('Incorrect pk count for {}: {}'
                                .format(table.name, len(keys)))
    for clm, ky in zip(table.primary_key, keys):
        try:
            ky = clm.type.python_type(ky)
        except NotImplementedError:
            # Lets hope string will do
            pass
        except ValueError:
            raise RequestParseError('Could not parse pk param {}'
                                    .format(ky))
        if where is not None:
            where = expression.and_(where, clm == ky)
        else:
            where = (clm == ky)
    if where is None:
        raise RequestParseError('Could not parse identifiers {}'.format(keys))
    return where


def called(context, name, args):
    sqlobj = context['sqlobj']
    if name in context['tables']:
        sqlobj = add_table(sqlobj, context['tables'][name])
        sqlobj = sqlobj.where(parse_pk(context['tables'][name], *args))
        context['sqlobj'] = sqlobj
        return

    raise NotFoundException()


def item(context, name):
    sqlobj = context['sqlobj']
    if name in context['tables']:
        context['sqlobj'] = add_table(sqlobj, context['tables'][name])
        return

    if hasattr(sqlobj, 'table'):
        table = sqlobj.table
        if name in table.columns:
            if isinstance(sqlobj, expression.Delete):
                # §10.3.8.2
                if not context['request_payload']:
                    context['sqlobj'] = table.update(
                        sqlobj._whereclause, {table.columns[name]: None})
                    return
                else:
                    raise RequestParseError('DELETE to a primative should not '
                                            'contain a payload')
            if isinstance(sqlobj, expression.Update):
                # §10.3.8.1
                context['sqlobj'] = sqlobj.values(
                    {table.columns[name]: context['request_payload']})
                return
            raise RequestParseError('Unexpected Primative')

    if isinstance(sqlobj, expression.Select):
        # §10.2.2
        # We search the nearest url elements first
        # For /users/locations/id we return locations.id not users.id
        for table in reversed(sqlobj.froms):
            if name in table.columns:
                context['sqlobj'] = sqlobj.column(table.columns[name])
                return
            cols = [col for col in table.columns if col.name == name]
            if len(cols) == 1:
                context['sqlobj'] = sqlobj.column(cols[0])
                return

    raise NotFoundException()


def count(context):
    # §10.2.4
    try:
        context['sqlobj'] = context['sqlobj'].count()
    except IndexError:
        context['sqlobj'] = context['sqlobj'].column('count(*)')
    except AttributeError:
        raise RequestParseError('Invalid $count')


def value(context):
    if (not isinstance(context['sqlobj'], expression.Select)
            or len(context['sqlobj'].columns) != 1):
        raise RequestParseError('Invalid $value')
    # §10.2.2.1
    context['response_headers']['Content-Type'] = 'text/plain'


def links(context, tables):
    # §10.2.5
    raise NotImplementedError()


def parse(url, context):
    parserfuncts = {
        'callable': functools.partial(called, context),
        'item': functools.partial(item, context),
        'count': functools.partial(count, context),
        'value': functools.partial(value, context),
        'links': functools.partial(links, context)}

    grammar = parsley.makeGrammar(url_grammar, parserfuncts)
    try:
        grammar(url).path()
    except parsley.ParseError:
        raise RequestParseError('Could not parse url')
