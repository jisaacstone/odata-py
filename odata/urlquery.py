import functools
from odata.shared import select_star, get_tables
from odata.exc import RequestParseError
from odata import filter_grammar


def parse_filter(context, value):
    return filter_grammar.parse(context, value)


def parse_expand(context, value):
    raise NotImplementedError()


formats = dict(
    xml="application/xml",
    json="application/json")


def parse_format(context, value):
    if context['headers'].get('Content-Type') == 'text/plain':
        raise RequestParseError("Cannot have $value and $format")
    if value in formats:
        context['headers']['Content-Type'] = formats[value]
    else:
        raise RequestParseError('Unsupported value for $format')


def parse_select(context, value):
    if value == '*':
        context['sqlobj'] = select_star(context['sqlobj'])
        return

    tables = get_tables(context['sqlobj'])

    col_names = [t.strip().replace(' ', '_') for t in value.split(',')]
    for name in col_names:
        if '.' in name:
            table, _, col = name.partition('.')
            if table not in tables:
                raise RequestParseError('Entity {} in $select but not '
                                        'present in url'.format(table))
            if col == '*':
                context['sqlobj'] = select_star(context['sqlobj'],
                                                tables[table].columns)
            else:    
                if col not in tables[table].columns:
                    raise RequestParseError('Entity {0} has not attribute {1}'
                                            .format(table, col))
                context['sqlobj'] = context['sqlobj'].column(
                    tables[table].columns[col])
        else:
            cols = [tbl.columns[name] for tbl in tables.values()
                    if name in tbl.columns]
            if not cols:
                raise RequestParseError('could not select {}'.format(name))
            if len(cols) > 1:
                raise RequestParseError('imprecise select {}'.format(name))
            context['sqlobj'] = context['sqlobj'].column(cols[0])


def parse_top(context, value):
    if isinstance(value, int) or (isinstance(value, basestring)
                                  and value.isdigit()):
        context['sqlobj'] = context['sqlobj'].limit(value)
    else:
        raise RequestParseError('invalid $top {}'.format(value))


def parse_skip(context, value):
    if isinstance(value, int) or (isinstance(value, basestring)
                                  and value.isdigit()):
        context['sqlobj'] = context['sqlobj'].offset(value)
    else:
        raise RequestParseError('invalid $skip {}'.format(value))


def parse_inlinecount(context, value):
    raise NotImplementedError()


def parse(context, qargs):
    parsers = {
        '$select': functools.partial(parse_select, context),
        '$expand': functools.partial(parse_expand, context),
        '$format': functools.partial(parse_format, context),
        '$filter': functools.partial(parse_filter, context),
        '$inlinecount': functools.partial(parse_inlinecount, context),
        '$top': functools.partial(parse_top, context),
        '$skip': functools.partial(parse_skip, context)}

    for param, value in qargs.items():
        if param.startswith('$'):
            try:
                parsers[param](value)
            except AttributeError:
                raise RequestParseError('Unsupported {}'.format(param))
            except ValueError:
                raise RequestParseError('Unknown value for {}'.format(param))
            except KeyError:
                raise RequestParseError('Undefined system query: {}'
                                        .format(param))

    return context
