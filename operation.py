class NoReduceOperation(Exception):
    '''
    No reduce operation
    '''
    pass


class Operation(object):
    '''
    Operations
    '''
    def __init__(self, map_operation, reduce_operation, return_address, write, read):
        # Operations
        self.map_operation = map_operation
        self.reduce_operation = reduce_operation

        # Return address
        self.return_address = return_address

        # Operation information
        self.write = write
        self.read = read

        # Operation resulsts
        self.map_results = {}

    def map(self, index, data_block):
        self.map_results[index] = self.map_operation(data_block)


    def reduce(self):
        '''
        Execute the operation on the given data
        '''
        results = []

        for i in self.map_results:
            results.append(self.map_results[i])

        return self.reduce_operation(results)
