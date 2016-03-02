import shelve
from multiprocessing import Queue
from operation import Operation
from dataset import Dataset


class Storage(object):
    '''
    Storage system
    '''
    def __init__(self):
        self.datamapper = shelve.open('metadata')
        self.data = shelve.open('data')


    def add_data(self, dataset_id, data_block):
        '''
        Add data to already existing data block
        '''
        print('Not implemented yet')


    def add_dataset(self, dataset_id, dataset):
        '''
        Add a new dataset to system
        '''
        # Add metadata about the dataset
        self.datamapper[dataset_id] = Dataset()

        # Add the data to the 'disk'
        for data_block in dataset:
            self.add_data(dataset_id, data_block)


    def add_operation(self, dataset_id, operation):
        '''
        Queue an operation to the execution-queue of a dataset
        '''
        self.datamapper[dataset_id].add_operation(operation)


    def run_queue(self, dataset_id):
        '''
        Run the execution-queue for a given dataset
        '''
        dataset = self.datamapper[dataset_id]

        while not dataset.queue.empty():
            operation = dataset.queue.get()
            for data_block in dataset.datablocks:
                operation.execute(self.data[data_block])
