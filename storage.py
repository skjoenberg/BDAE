'''
bla bla bla
'''
import mmap
import time
import os
import random
import string
from multiprocess import Manager, Process, Queue
from operation import Operation
from dataset import Dataset
import random
import string
import math

class Storage(object):
    '''
    Storage system
    '''
    def __init__(self):
        # The given page size
        self._PAGE_SIZE = 4096

        # The given size for data blocks
        self._BLOCK_SIZE = 1 * self._PAGE_SIZE

        # Meta data about datasets
        self.dataset_table = {} # Skal gemmes på en måde

        # Read/write head position
        self.position = 0

        # Manager for concurrency
        self.manager = Manager()

        # Job-queue for reading data
        self.job_queue = self.manager.list()

        # Data queueueueuueue
        self.data_queues = self.manager.dict()

        # Path to storage file
        _path = 'data.data'

        # Size of storage (Default 200 mb)
        self._SIZE   = 4096 * 256 * 200

        # Amount of blocks
        self._BLOCKS = math.floor(self._SIZE / self._BLOCK_SIZE)

        # Check whether a storage file exists, else create one
        if not os.path.exists(_path):
            print('Writing storage file')
            f = open(_path, 'w+b')
            f.write(b'?' * self._SIZE)
            f.close

        # Open storage and create a MMAP
        try:
            storage = open(_path, 'a+b')
        except:
            print('Cannot open storage file!')

        self.datamap = mmap.mmap(storage.fileno(), 0)

        # Free space vector
        self.free_space =[(0, self._BLOCKS)]

        # Index to where the lastest data block have been written
        self._index = 0

    def _write_data(self, address, data_block):
        '''
        Writes a data block to the page at the given address
        '''
        print('¤ Writing data block at ' + str(address))
        try:
            # Go to the current address
            self.datamap.seek(address)
            self.position = address

            # Write the block
            self.datamap.write(bytes(data_block, 'utf-8'))
        except:
            print('! Could not write data block to ' + str(address) + '. Not enough space.')

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
        print('+ Reading data from ' + str(address))
        data = ''
        try:
            # Go to the current address
            self.datamap.seek(address)
            self.position = address

            # Read the data
            data = self.datamap.read(self._PAGE_SIZE)
        except:
            print('Could not read data block from ' + str(address))

        return data

    def _worst_fit(self, n_blocks):
        largest_segment = sorted(self.free_space, key=lambda x: x[1])[0]
        blocks_amount = largest_segment[1]

        assert blocks_amount >= n_blocks

        free_blocks = []
        current_block = largest_segment[0]
        for _ in range(n_blocks):
            free_blocks.append(current_block)
            current_block += self._BLOCK_SIZE

        self.free_space.remove(largest_segment)
        self.free_space.append((current_block, blocks_amount - n_blocks))

        return free_blocks


    def _request_blocks(self, n_blocks):
        return self._worst_fit(n_blocks)

    def get_size(self, dataset_id):
        '''
        Get the amount of blocks in a dataset
        '''
        return self.dataset_table[dataset_id].size

    def append_data(self, dataset_id, data_block, address):
        '''
        Append data to an existing dataset
        '''
        # Check if there is any more allocated space
        # for the dataset
        if self.dataset_table[dataset_id].space_left():
            # Write data block and increament size
            self._write_data(address, data_block)
            self.dataset_table[dataset_id].size+=1
            return address

    def add_dataset(self, dataset_id, dataset, size=None):
        '''
        Add a new dataset to the storage
        '''
        # Add metadata about the dataset
        if size:
            current_size = size
        else:
            current_size = len(dataset)

        self.dataset_table[dataset_id] = Dataset(current_size)

        requested_blocks = self._request_blocks(current_size)

        assert len(requested_blocks) >= len(dataset)

        # Write the data blocks to a file
        block_index = 0
        for data_block in dataset:
            self.append_data(dataset_id, data_block, requested_blocks[block_index])
            self.dataset_table[dataset_id].append_block_index(requested_blocks[block_index])
            block_index += 1


    def read_data(self, dataset_id, data_queue):
        '''
        Run the execution-queue for a given dataset
        '''
        # Generate a random id (6 characters)
        data_id = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(6))

        dataset = self.dataset_table[dataset_id]

        self.data_queues[data_id] = data_queue

        for address in dataset.datablocks:
            self.job_queue.append((address, data_id))

        return dataset.datablocks

    def reader(self):
        '''
        A reading process, which serves data blocks requests from read_data
        '''
        while True:
            # Sort the list of jobs by their address
            jobs = sorted(self.job_queue, key=lambda x: x[0])

            try:
                # Find the job with the closest highest address
                (address, data_id) = next(x for x in jobs if x[0] >= self.position)

                # Read the data from disc
                data = self._read_block(address)

                # Serve data to the requesting process
                self.data_queues[data_id].put(data)

                # Remove the job from the list
                self.job_queue.remove((address, data_id))
            except:
                # No jobs found. Start from position 0.
                self.position = 0
                time.sleep(0.01)
