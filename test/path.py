from itertools import starmap
from sqlalchemy import select
from odata import parser, urlpath
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


@pytest.mark.parametrize('verb,path,query', expected)
def test_expected(verb, path, query):
    context = dict(sqlobj=parser.verbfuncs[verb], headers={}, tables=tables,
                   request_payload=None)
    result = urlpath.parse(path, context)
    assert unicode(context['sqlobj']) == query
