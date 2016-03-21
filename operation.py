import dill as pickle

class MapFunctionError(Exception):
    '''
    No reduce operation
    '''
    pass

class ReduceFunctionError(Exception):
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

#        @staticmethod
    def map(self, data_block):
        '''
        Execute the map-function on the given data block
        '''
        try:
#            self.map_results[index] = self.map_operation(data_block)
            def map_wrapper(): pass

            map_wrapper.__code__ = pickle.loads(self.map_operation)
            map_operation = map_wrapper

            return map_wrapper(data_block)
#            return self.map_operation(data_block)
        except:
            raise MapFunctionError

    def reduce(self, results):
        '''
        Reduce the resulsts of the map-function
        '''
        def reduce_wrapper(): pass

        reduce_wrapper.__code__ = pickle.loads(self.reduce_operation)
        reduce_operation = reduce_wrapper

        try:
            reduced_result = reduce_operation(results)
        except:
            raise ReduceFunctionError

        return reduced_result
