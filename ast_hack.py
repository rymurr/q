
import q
import ast
class Test(ast.NodeVisitor):
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
        return node.id
    def visit_Add(self, node):    
        return '+'
    
connection = q.connect()
def test_parse(query):
    xxx = ast.parse(query)
    return connection.execute(Test().visit_BinOp(xxx.body[0].value))
test_parse('3*2+3')
