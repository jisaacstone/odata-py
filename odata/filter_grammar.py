import parsley
import operator
from sqlalchemy import func
from sqlalchemy.sql import expression
from sqlalchemy.sql.functions import concat, char_length, operators
from odata.exc import RequestParseError


gops = {
    'eq': operator.eq,
    'ne': operator.ne,
    'gt': operator.gt,
    'lt': operator.lt,
    'le': operator.le,
    'and': operator.and_,
    'or': operator.or_,
    'not': operator.not_,
    'add': operator.add,
    'sub': operator.sub,
    'mul': operator.mul,
    'div': operator.truediv,
    'mod': operator.mod}


def literals(fn):
    def wrapped(*args):
        largs = []
        for arg in args:
            if not isinstance(arg, expression.ClauseElement):
                arg = expression.literal(arg)
            largs.append(arg)
        return fn(*largs)
    return wrapped


@literals
def fun_substringof(searchstr, searchin):
    return searchin.contains(searchstr)


@literals
def fun_indexof(searchin, searchstr):
    raise NotImplementedError()


@literals
def fun_replace(base, find, replace):
    raise NotImplementedError()


@literals
def fun_substring(string, pos, length=None):
    raise NotImplementedError()


@literals
def fun_day(datetime):
    raise NotImplementedError()


@literals
def fun_hour(datetime):
    raise NotImplementedError()


@literals
def fun_minute(datetime):
    raise NotImplementedError()


@literals
def fun_second(datetime):
    raise NotImplementedError()


@literals
def fun_year(datetime):
    raise NotImplementedError()


@literals
def fun_isof(obj, compareto=None):
    raise NotImplementedError()


gfuns = dict(
    substringof=fun_substringof,
    endswith=literals(operators.endswith_op),
    startswith=literals(operators.startswith_op),
    length=literals(char_length),
    index_of=fun_indexof,
    replace=fun_replace,
    tolower=literals(func.lower),
    toupper=literals(func.toupper),
    trim=literals(func.trim),
    concat=literals(concat),
    day=fun_day,
    hour=fun_hour,
    minute=fun_minute,
    second=fun_second,
    year=fun_year,
    round=literals(func.round),
    floor=literals(func.floor),
    ceiling=literals(func.ceil),
    isof=fun_isof)


filter_grammar = r'''
escapedChar = '\\' ('\'')
integer = <digit+>:ds -> int(ds)
float = (integer '.' integer):fs -> float(fs)
number = (integer | float)
boolean = ('true' | 'false'):value -> value == 'true'
string = '\'' (escapedChar | ~'\'' anything)*:c '\'' -> u''.join(c)
parens = '(' ws expr:e ws ')' -> e
column = <(letterOrDigit | '_')+>:name -> get_column(name)
null = 'null' -> None
atom = ws (parens | number | boolean | string | null | column)
op = ws <letter+>:name ws atom:e ?(name in ops) -> ops[name], e
expr = atom:left op*:right -> compose(left, right)
'''


def compose(left, right):
    if not right:
        return left
    op, value = right.pop(0)
    if op in (operator.and_, operator.or_):
        return op(left, compose(value, right))
    return compose(op(left, value), right)


def colgetter(sqlobj):
    def getcol(name):
        for tbl in sqlobj.froms:
            for col in tbl.columns:
                if col.name.lower() == name.lower():
                    return col
        raise RequestParseError('{} is unknown'.format(name))
    return getcol


def parse(context, value):
    grammar = parsley.makeGrammar(
        filter_grammar,
        dict(compose=compose,
             ops=gops,
             get_column=colgetter(context['sqlobj'])))
    context['sqlobj'] = context['sqlobj'].where(grammar(value).expr())
