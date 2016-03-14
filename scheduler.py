from multiprocessing import Manager
from operation import Operation
from priorities import Priority
from multiprocessing import Pool, Process, Queue
import pickle
import time


class Scheduler(object):
    '''
    Scheduler
    '''
    def __init__(self, storage, threads):
        # Manager for concurrency
        self.manager = Manager()

        # System storage
        self.storage = storage

        # Queues
        self.high_access = self.manager.list([])
        self.normal_access = self.manager.list([])
        self._pool = Pool(processes=threads)

        # Operations
        self.operation_map = {}

    def _map_operation(self, dataset_id, operation):
        if dataset_id in self.operation_map:
            self.operation_map[dataset_id].append(operation)
        else:
            self.operation_map[dataset_id] = [operation]

    def add_operation(self, dataset_id, prio, map_operation, reduce_operation=None, return_address=None, write=False, read=True):
        '''
        Add operation to a dataset
        '''
        operation = Operation(map_operation, reduce_operation, return_address, write, read)

        # Add the operation to queue
        self._map_operation(dataset_id, operation)

        # Add data block to scheduler
        #!!! Remember to check, whether the block is in the normal queue, when adding it to the high queue
        if prio == Priority.high:
            if dataset_id not in self.high_access:
                self.high_access.append(dataset_id)

                if dataset_id in self.normal_access:
                    self.normal_access.remove(dataset_id)

        elif prio == Priority.normal:
            if dataset_id not in self.normal_access and dataset_id not in self.high_access:
                self.normal_access.append(dataset_id)


    def run_queue(self, dataset_id):
        data = Queue()

        hej = Process(target=self.storage.read_data, args=(dataset_id, data))

        hej.start()
        for i in range(self.storage.get_size(dataset_id)):
            data_block = data.get()
            print('- Performing operations on block: ' + str(i) + ', dataset: ' + dataset_id)
            for operation in self.operation_map[dataset_id]:
#                print(operation)
                operation.map(i, data_block)
#                self._pool.apply(operation.map, (i, data_block, ))

        for operation in self.operation_map[dataset_id]:
            operation.reduce()

        del self.operation_map[dataset_id]

    def schedule(self):
        '''
        Schedule the queued operations
        '''

        while True:
            print('/ High priority queue is ' + str(self.high_access))
            print('/ Normal priority queue is ' + str(self.normal_access))
            if self.high_access:
                self.run_queue(self.high_access.pop(0))
            elif self.normal_access:
                self.run_queue(self.normal_access.pop(0))
            else:
                break
