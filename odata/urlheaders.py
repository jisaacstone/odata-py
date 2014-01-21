#coding: utf-8
import functools
from sqlalchemy.sql import expression
from .exc import RequestParseError


content_types = dict(
    json='application/json',
    xml='application/xml',
    atom='application/atom_xml')


def parse_content_type(context, value):
    # §8.1.2
    #TODO
    pass


def parse_accept(context, value):
    # §8.2.3
    if value in content_types:
        context['response_headers']['Content-Type'] = \
            context['response_headers'].get(
                'Content-Type', content_types[value])


def parse_location(context, value):
    # §8.3.2
    raise NotImplementedError()


def parse_prefer(context, value):
    # §8.4.1
    context['response_headers']['Preference-Applied'] = value
    if value == 'return-no-content':
        context['response_status'] = 204
    elif value == 'return-content':
        if isinstance(context['sqlobj'], expression.Insert):
            context['response_status'] = 201
        else:
            context['response_status'] = 200
        sqlobj = context['sqlobj']
        # This will throw an error at compile time if the dialect
        # does not support returning
        context['sqlobj'] = sqlobj.returning(*sqlobj.table.columns)
    else:
        raise RequestParseError('invalid value for Prefer header')


def parse(context):
    headers = context['request_headers']
    parsers = {
        'content-type': functools.partial(parse_content_type, context),
        'accept': functools.partial(parse_accept, context),
        'location': functools.partial(parse_location, context),
        'prefer': functools.partial(parse_prefer, context)}

    for header, value in headers.iteritems():
        if header.lower() in parsers:
            parsers[header.lower()](value)
