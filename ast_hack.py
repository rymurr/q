
import q
import ast
from cStringIO import StringIO

def translate(command):
    node = ast.parse(command)
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
            statement += '?[' + self.visit(node.func.value) + ';(' + ';'.join(map(self.visit, node.args)) + ');0b;()]'
        else:
            raise NotImplemented
        return statement    

    def visit_Eq(self, node):
        return '='

    def visit_In(self, node):
        return 'in'

    def visit_Compare(self, node):
        left = self.visit(node.left)
        ops = list(map(self.visit, node.ops))
        comparators = list(map(self.visit, node.comparators))
        return ';'.join(('({0};{1};{2})'.format(op, left, comparator) for op, comparator in zip(ops, comparators)))

    def visit_List(self, node):
        return 'enlist ' + ' '.join(list(map(self.visit, node.elts)))
def main():     
    connection = q.connect()
    connection.execute(translate('3*2+3'))
