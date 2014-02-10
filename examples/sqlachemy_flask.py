import flask

from odata import parser
from odata import exc

from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool


# Table Defs
metadata = MetaData()
users = Table(
    'users', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('fullname', String),
)
addresses = Table(
    'addresses', metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', None, ForeignKey('users.id')),
    Column('email_address', String, nullable=False)
)
# in-memeory db
engine = create_engine('sqlite://',
                       connect_args={'check_same_thread': False},
                       poolclass=StaticPool)
# create the tables in the db
metadata.create_all(engine)
# this will handle the odata path, headers, query args parsing
request_parser = parser.RequestParser(
    tables=dict(users=users, addresses=addresses),
    engine=engine)

# Fill with some demo data
connection = engine.connect()
connection.execute(users.insert(), [
    {'id': 0, 'name': 'Kale', 'fullname': 'Kale-i-o'},
    {'id': 1, 'name': 'Broke', 'fullname': 'Broke Lin'},
    {'id': 2, 'name': 'Grale', 'fullname': 'Grale Muhammad Berners-lee'},
    {'id': 3, 'name': 'Kale', 'fullname': 'Kale Number 2'}])
connection.execute(addresses.insert(), [
    {'id': 0, 'user_id': 1, 'email_address': 'lin@example.com'},
    {'id': 1, 'user_id': 3, 'email_address': 'k2+funguy@example.com'}])


# Flask stuff
app = flask.Flask(__name__)


# Error handling
@app.errorhandler(exc.RequestParseError)
def request_parse_error(err):
    return flask.jsonify({'error': err.message}, 400)


@app.errorhandler(NotImplementedError)
def not_implemented_error(err):
    return flask.jsonify({'error': 'Feature not yet implemented'}, 400)


@app.errorhandler(exc.NoContent)
def no_content(err):
    return flask.Response(status=err.code)


# Try hitting localhost:5000/demo.svc/users?$top=3
@app.route('/demo.svc/<path:odata_path>')
def route(odata_path):
    kwargs = dict(
        path=odata_path,
        http_verb=flask.request.method,
        headers=flask.request.headers,
        query_args=flask.request.args,
        payload=flask.request.form
                if flask.request.form
                else flask.request.data)

    response = request_parser.parse(**kwargs)
    return flask.jsonify(dict(d=response['payload']),
                         status=response['status'],
                         headers=response['headers'])


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
