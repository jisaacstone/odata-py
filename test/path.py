from itertools import starmap
from sqlalchemy import select
from odata import parser
from fixtures import tables, users, addresses

expected = [
    ('GET', '/users', unicode(users.select())),
    ('GET', '/users(5)', unicode(
        users.select().where(users.c.id == 5))),
    ('GET', '/users(5)/addresses', unicode(
        select().select_from(users.join(addresses)).where(users.c.id == 5)
        .column(addresses.c.id).column(addresses.c.user_id)
        .column(addresses.c.email_address))),
    ('GET', '/addresses/user_id', unicode(
        select().select_from(addresses).column(addresses.c.user_id))),
    ('DELETE', '/users(5)', unicode(users.delete().where(users.c.id == 5))),
    ('DELETE', '/users(5)/fullname', unicode(
        users.update().where(users.c.id == 5).values({
            users.c.fullname: None}))),
    ]

no_validation = [
    ('POST', '/users', unicode(users.insert())),
    ('PUT', '/users', unicode(users.update())),
    ]


class TestRequestParser(parser.RequestParser):
    def __init__(self):
        super(TestRequestParser, self).__init__(tables)

    def parse(self, verb, path, expected):
        self.expected = expected
        return super(TestRequestParser, self).parse(path, verb)

    def query(self, sqlobj):
        return unicode(sqlobj)

    def render(self, context):
        assert context['payload'] == self.expected


def test():
    prsr = TestRequestParser()
    mp = list(starmap(prsr.parse, expected))
    print '\n', len(mp), 'tests run'

def test_no_validation():
    # Normally we would fail for lack of payload
    # So mock the validation to do nothing!
    parser.validate_and_cleanup = lambda a, b: a

    prsr = TestRequestParser()
    mp = list(starmap(prsr.parse, no_validation))
    print '\n', len(mp), 'tests run'
