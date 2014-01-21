import functools
from odata.shared import select_star
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
    if context['headers'].get('content-type') == 'text/plain':
        raise RequestParseError("Cannot have $value and $format")
    if value in formats:
        context['headers']['content-type'] = formats[value]
    else:
        raise RequestParseError('Unsupported value for $format')


def parse_select(context, value):
    # TODO: does not handle dot identifiers eg Item.foo or Item.*
    if value == '*':
        context['sqlobj'] = select_star(context['sqlobj'])
        return

    col_names = [t.strip().replace(' ', '_') for t in value.split(',')]
    for name in col_names:
        for tbl in context['sqlobj'].froms + [None]:
            if tbl is None:
                raise RequestParseError('{} not understood'.format(name))
            if name in tbl.columns:
                context['sqlobj'] = context['sqlobj'].column(tbl.columns[name])
                break
            cols = [c for c in tbl.columns if c.name == name]
            if len(cols) == 1:
                context['sqlobj'] = context['sqlobj'].column(cols[0])
                print context['sqlobj']
                break


def parse_top(context, value):
    context['sqlobj'] = context['sqlobj'].limit(value)


def parse_skip(context, value):
    context['sqlobj'] = context['sqlobj'].offset(value)


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
