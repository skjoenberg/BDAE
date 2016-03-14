from multiprocessing import Queue


class Dataset(object):
    '''
    A set of data
    '''

    def __init__(self, max_size):
        # Addresses of datablocks
        self.datablocks = []

        # Dataset size information
        self.size = 0
        self.max_size = max_size

        # Number of write operations
        self.writes = 0

    def append_block_index(self, index):
        '''
        Append a data block to the dataset
        '''
        self.datablocks.append(index)

    def space_left(self):
        '''
        Returns whether the dataset has space left or not
        '''
        return self.size < self.max_size

    def last_block(self):
        '''
        Returns an address to next free data_block
        '''
        return self.datablocks[-1]
