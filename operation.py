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

    def map(self, data_block):
        '''
        Execute the map-function on the given data block
        '''
        try:
#            self.map_results[index] = self.map_operation(data_block)

            # Create wrapper function
            def map_wrapper(): pass

            # Put code into the wrapper function
            map_wrapper.__code__ = pickle.loads(self.map_operation)
            # Use the wrapper as map-function
            return map_wrapper(data_block)
        except:
            raise MapFunctionError

    def reduce(self, results):
        '''
        Reduce the resulsts of the map-function
        '''
        # Create wrapper function
        def reduce_wrapper(): pass

        # Put code into the wrapper function
        reduce_wrapper.__code__ = pickle.loads(self.reduce_operation)

        try:
            # Use the wrapper as reduce-function
            reduced_result = reduce_wrapper(results)
        except:
            raise ReduceFunctionError

        return reduced_result
