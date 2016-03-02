from multiprocessing import Queue


class Dataset(object):
    '''
    A set of data
    '''

    def __init__(self):
        self.datablocks = []
        self.queue = Queue()


    def add_operation(self, operation):
        '''
        Add a process to the queue
        '''
        self.queue.put(operation)
