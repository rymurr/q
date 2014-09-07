import ast
import astunparse
from cStringIO import StringIO

class Transformer(ast.NodeTransformer):
    def __init__(self):
        super(Transformer, self).__init__()

    def visit_Call(self, node):
        child = self.visit(node.func)
        if isinstance(child.value, ast.Call):
            keyword = nameAndArgsToKeyword(node.func.attr, node.args+node.keywords)
            isThere = appendKeyword(child.value.keywords, keyword)
            if not isThere:
                child.value.keywords.append(keyword)
            return child.value 
        else:
            return node

def appendKeyword(keywords, keyword):
    for k in keywords:
        if k.arg == keyword.arg:
            combine(k, keyword)
            return True
    return False    

def combine(keyword, newKeyword):
    if isinstance(keyword.value, ast.List):
        keyword.value.elts.extend(newKeyword.value.elts)
    elif isinstance(keyword.value, ast.Dict):
        keyword.value.keys.extend(newKeyword.value.keys)
        keyword.value.values.extend(newKeyword.value.values)

groupbySet = set(['groupby',])
aggregationSet = set(['last','mean'])
def nameAndArgsToKeyword(name, args):
    if name in groupbySet:
        arg = 'groupby'
        keyword = ast.keyword(arg=arg, value = ast.Dict(keys=[args], values=[args]))
    elif name in aggregationSet:
        arg = 'aggregate'
        keyword = ast.keyword(arg=arg, value = ast.Dict(keys=[ast.Str(name),], values=[ast.List(elts=args)]))
    else:
        raise NotImplementedError
    return keyword

def transformHelper(command):
    node = preprocess(command)
    print ast.dump(node)
    try:
        print astunparse.unparse(node)
    except:
        pass
    return node

def preprocess(command):
    transform = Transformer()
    node = ast.parse(command)
    return transform.visit(node)
    
def translate(command):
    node = preprocess(command)
    parser = Parser()
    parser.visit(node)
    return parser.writer.getvalue()

class Parser(ast.NodeVisitor):

    def __init__(self):
        super(Parser, self).__init__()
        self.writer = StringIO()

    def visit(self, node):
        print node
        return super(Parser, self).visit(node)

    def visit_Mult(self, node):
        return '*'

    def visit_BinOp(self, node):
        left = self.visit(node.left)
        right = self.visit(node.right)
        op = self.visit(node.op)
        return '{0}[{1};{2}]'.format(op, left, right)

    def visit_Num(self, node):
        return node.n

    def visit_Name(self, node):
        return '`' + node.id

    def visit_Add(self, node):    
        return '+'

    def visit_Expr(self, node):
        self.writer.write(self.visit(node.value) + '\n')

    def visit_Module(self, node):
        list(map(self.visit, node.body))

    def visit_Call(self, node):
        attr = node.func.attr
        statement = ''
        if (attr == 'select'):
            table = self.visit(node.func.value)
            constraints = ';'.join(map(self.visit, node.args))
            groupby = ';'.join(map(self.visit, [i for i in node.keywords if i.arg == 'groupby']))
            #aggregation = 
            statement += '?[' + self.visit(node.func.value) + ';(' + ';'.join(map(self.visit, node.args)) + ');0b;()]'
        else:
            raise NotImplementedError
        return statement    

    def visit_Eq(self, node):
        return '='

    def visit_In(self, node):
        return 'in'

    def visit_Str(self, node):
        return '`'+node.s

    def visit_Dict(self, node):
        return '(' + ' '.join(map(self.visit, node.keys)) + ')!(' + ' '.join(map(self.visit, node.values)) + ')'

    def visit_Compare(self, node):
        left = self.visit(node.left)
        ops = list(map(self.visit, node.ops))
        comparators = list(map(self.visit, node.comparators))
        return ';'.join(('({0};{1};{2})'.format(op, left, comparator) for op, comparator in zip(ops, comparators)))

    def visit_List(self, node):
        return 'enlist ' + ' '.join(list(map(self.visit, node.elts)))
