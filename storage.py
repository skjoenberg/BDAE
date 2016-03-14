'''
bla bla bla
'''
import mmap
import time
from multiprocessing import Process, Queue
from operation import Operation
from dataset import Dataset


class Storage(object):
    '''
    Storage system
    '''
    def __init__(self):
        self._PAGE_SIZE = 4096

        self.dataset_table = {} # Skal gemmes på en måde

        try:
            storage = open('data.data', 'a+b')
        except:
            print('Cannot open storage file!')
            pass

        self.datamap = mmap.mmap(storage.fileno(), 0)

        self._index = 0


    def _write_data(self, address, data_block):
        '''
        Writes data to a given address
        '''
        # Go to the current address
        print('Writing data to ' + str(address))
        self.datamap.seek(address)

        # Write the block
        self.datamap.write(bytes(data_block, 'utf-8'))
#        self.datamap.write(bytes('hue','utf-8'))

        # Flush the written data to the file
        try:
            self.datamap.flush()
        except:
            print("Cannot flush data with mmap!")
            pass

    def _read_block(self, address):
        '''
        Writes data to a given address
        '''
        print('- Reading data from ' + str(address))
        # Go to the current address
        self.datamap.seek(address)

        # Return the read data
        return self.datamap.read(self._PAGE_SIZE)

    def get_size(self, dataset_id):
        return self.dataset_table[dataset_id].size

    def append_data(self, dataset_id, data_block, address):
        '''
        Append data to an existing dataset
        '''
        # Check if there is any more allocated space
        # for the dataset
        if self.dataset_table[dataset_id].space_left():
            self._write_data(address, data_block)
            self.dataset_table[dataset_id].size+=1
            return address

    def add_dataset(self, dataset_id, dataset, size):
        '''
        Add a new dataset to system
        '''
        # Add metadata about the dataset
        self.dataset_table[dataset_id] = Dataset(size)

        # Generate naive start-address
        address = self._index * self._PAGE_SIZE

        # Write the data blocks to a file
        for data_block in dataset:
            self.append_data(dataset_id, data_block, address)
            self.dataset_table[dataset_id].append_block_index(address)
            address += self._PAGE_SIZE

        self._index += size

    def read_data(self, dataset_id, data):
        '''
        Run the execution-queue for a given dataset
        '''
        dataset = self.dataset_table[dataset_id]

        # Write operation
        # write = None

        for address in dataset.datablocks:
            data.put(self._read_block(address))

        return dataset.datablocks
