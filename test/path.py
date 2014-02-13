from itertools import product, chain
from sqlalchemy import select
from odata import parser, urlpath
from odata.exc import NotFoundException, RequestParseError
from fixtures import tables, users, addresses
import pytest

expected = [
    ('GET', '/users', unicode(select().select_from(users))),
    ('GET', '/users(5)', unicode(
        select().select_from(users).where(users.c.id == 5))),
    ('GET', '/users(5)/addresses', unicode(
        select().select_from(users.join(addresses)).where(users.c.id == 5))),
    ('GET', '/addresses/user_id', unicode(
        select().select_from(addresses).column(addresses.c.user_id))),
    ('DELETE', '/users(5)', unicode(users.delete().where(users.c.id == 5))),
    ('DELETE', '/users(5)/fullname', unicode(
        users.update().where(users.c.id == 5).values({
            users.c.fullname: None}))),
    ('POST', '/users', unicode(users.insert())),
    ('PUT', '/users', unicode(users.update())),
    ]


def make_context(verb):
    return dict(sqlobj=parser.verbfuncs[verb],
                response_headers={},
                tables=tables,
                request_payload=None)


@pytest.mark.parametrize('verb,path,query', expected)
def test_expected(verb, path, query):
    context = make_context(verb)
    urlpath.parse(path, context)
    assert unicode(context['sqlobj']) == query


def test_value():
    context = make_context('GET')
    path = '/users/name/$value'
    urlpath.parse(path, context)
    assert unicode(context['sqlobj']) == unicode(
        select().select_from(users).column(users.c.name))
    assert 'Content-Type' in context['response_headers']
    assert context['response_headers']['Content-Type'] == 'text/plain'


raise404urls = ['/nope', '/users/nope']
raise404 = product(parser.verbfuncs.keys(), raise404urls)


@pytest.mark.parametrize('verb,path', raise404)
def test_raise404(verb, path):
    context = make_context(verb)
    with pytest.raises(NotFoundException):
        urlpath.parse(path, context)


raiserpe = chain(
    product(['POST', 'PUT', 'PATCH', 'MERGE', 'DELETE'],
            ['/users/id/$value', 'users/$count']),
    [('GET', '/users/$value'),
     ('GET', '/$value'),
     ('GET', '/$count'),
     ])


@pytest.mark.parametrize('verb,path', raiserpe)
def test_raiserpe(verb, path):
    context = make_context(verb)
    with pytest.raises(RequestParseError):
        urlpath.parse(path, context)
