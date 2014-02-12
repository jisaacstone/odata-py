from sqlalchemy import select
from fixtures import users, addresses
from odata import urlquery

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
     unicode(users.select()))
    ])
def test(context, qargs, expected):
    try:
        ctx = urlquery.parse(context, qargs)
    except NotImplementedError:
        pytest.skip("not implemented")
    else:
        assert unicode(ctx['sqlobj']) == expected
