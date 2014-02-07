from json import JSONEncoder, dumps
from odata.exc import RequestParseError


class ReprEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (list, dict, str, unicode, int,
                            float, bool, type(None))):
            return JSONEncoder.default(self, obj)
        return repr(obj)


def jsonify(payload):
    return dumps(payload, cls=ReprEncoder)


def plaintext(payload):
    if hasattr(payload, 'values'):
        payload = payload.values()
    if isinstance(payload, list) or isinstance(payload, tuple):
        if len(payload) != 1:
            raise RequestParseError('No plaintext support for this endpoint')
        payload = payload[0]
    return payload


#TODO: 'application/xml', 'application/xml+atom'
formatters = {
    'application/json': jsonify,
    'text/plain': plaintext}


def payload(context):
    headers = context.get('headers', {})
    code = context.get('response_code', 200)
    ct = context.get('content_type',
                     headers.get('Content-Type', 'application/json'))
    if ct not in formatters:
        raise NotImplementedError
    payload = formatters[ct](context.get('payload', ''))
    return payload
