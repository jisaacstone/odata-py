from sqlalchemy import select
from fixtures import users, addresses
from odata import urlquery
from odata.exc import RequestParseError

import pytest


def context(sqlobj):
    return dict(
        sqlobj=sqlobj,
        headers={})

def S():
    return select().select_from(users)


def U():
    return users.update()


@pytest.mark.parametrize('context,qargs,expected', [
    (context(S()),
     {'$select': 'id'},
     unicode(S().column(users.c.id))),
    (context(S()),
     {'$select': '*'},
     unicode(users.select())),
    (context(S()),
     {'$select': 'users.id'},
     unicode(S().column(users.c.id))),
    (context(S()),
     {'$select': 'users.*'},
     unicode(users.select())),
    (context(select().select_from(users.join(addresses))),
     {'$select': 'name'},
     unicode(select([users.c.name]).select_from(users.join(addresses)))),
    (context(S()),
     {'$select': 'name,fullname'},
     unicode(select([users.c.name, users.c.fullname], None, [users]))),
    (context(S()),
     {'$top': '5'},
     unicode(S().limit(5))),
    (context(S()),
     {'$skip': '5'},
     unicode(S().offset(5))),
    ])
def test(context, qargs, expected):
    try:
        ctx = urlquery.parse(context, qargs)
    except NotImplementedError:
        pytest.skip("not implemented")
    else:
        assert unicode(ctx['sqlobj']) == expected


@pytest.mark.parametrize('context,qargs', [
    (context(S()), {'$select': 'notacolumn'}),
    (context(S()), {'$select': ''}),
    (context(S()), {'$select': None}),
    (context(S()), {'$select': 'notatable.id'}),
    (context(users.join(addresses).select()), {'$select': 'id'}),
    (context(U()), {'$select': 'name'}),
    (context(S()), {'$top': 'foo'}),
    (context(S()), {'$top': None}),
    (context(S()), {'$top': ''}),
    (context(S()), {'$skip': 'foo'}),
    (context(S()), {'$skip': None}),
    (context(S()), {'$skip': ''}),
    ])
def test_req_parse_error(context, qargs):
    try:
        urlquery.parse(context, qargs)
    except NotImplementedError:
        pytest.skip("not implemented")
    except RequestParseError:
        # expected
        pass
    else:
        pytest.fail('DID NOT RAISE {} {}'.format(context, qargs))
