from IPython.kernel.zmq.kernelbase import Kernel
import json
import pandas
import q

class KdbKernel(Kernel):
    implementation = 'Kdb'
    implementation_version = '0.1'
    language = 'kdb-q'
    language_version = '3.1'
    language_info = {'mimetype': 'text/plain'}
    banner = "Kdb+ kernel"

    def do_execute(self, code, silent, store_history=True, user_expressions=None,
                   allow_stdin=False):
        codeList = code.split('\n')
        pyCommands = [i for i in codeList if '.py' == i[:3]]
        qCommands = '\n'.join([i for i in codeList if not '.py' == i[:3]])
        returnCode = self.getPyResults(pyCommands) + self.getQResults(qCommands)
        if not silent:
            stream_content = {'name': 'stdout', 'text': returnCode}
            self.send_response(self.iopub_socket, 'stream', stream_content)

        return {'status': 'ok',
                # The base class increments the execution count
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {},
               }

    def getPyResults(self, pyCommands):
        returnString = ''
        for command in pyCommands:
            if 'connection_details' in command:
                self.connection_details = getDetails(command)
                self.connection = connect(self.connection_details)
                connectStr = 'Connection to q at {0} made successfully\n' if self.connection  else 'Connection to q at {0} failed\n'
                returnString += connectStr.format(self.connection_details)
        return returnString

    def getQResults(self, qCommands):
        if len(qCommands) == 0:
           return '' 
        if not self.connection:
            return 'No connection or connection details'
        try:
            result = self.connection.execute(qCommands.encode('ascii'))
        except Exception as e:
            print e
            print qCommands.encode('ascii')
            result = ''
        return formatQ(result)

def formatQ(result):
    formatter = getattr(result,'to_html', None)
    if formatter:
        return formatter()
    return str(result)

def connect(details):
    try:
        return q.connect(details['hostname'].encode('ascii'), details['port'], details['username'].encode('ascii'), details['password'].encode('ascii'))
    except Exception as e:
        print e
        return None

def getDetails(connectionStr):
    connectionDetail = connectionStr.split('=')[-1]
    print connectionDetail
    connectionObj = json.loads(connectionDetail)
    for detail in ('hostname', 'port', 'username', 'password'):
        if detail not in connectionObj:
            return None
    return connectionObj

if __name__ == '__main__':
    from IPython.kernel.zmq.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=KdbKernel)
