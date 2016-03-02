class Operation(object):
    '''
    Operations
    '''
    def __init__(self, executable, return_address):
        self.executable = executable
        self.return_address = return_address

    def execute(self, data):
        '''
        Execute the operation on the given data
        '''
        print('Not implemented yet')
