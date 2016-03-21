from multiprocessing import Manager
from operation import Operation
from priorities import Priority
from multiprocess import Semaphore, Pool, Process, Queue
import dill as pickle
import time
import random
import string


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
        self.operation_table = self.manager.dict()
#        self.operation_table = {}


    def add_operation(self, dataset_id, prio, map_operation, reduce_operation=None, return_address=None, write=False, read=True):
        '''
        Add operation to a dataset
        '''

        operation = Operation(map_operation, reduce_operation, return_address, write, read)

        # Add the operation to queue
        if dataset_id in self.operation_table:
            temp = self.operation_table[dataset_id]
            temp.append(operation)
            self.operation_table[dataset_id] = temp
        else:
            self.operation_table[dataset_id] = [operation]

        # Add data block to scheduler
        if prio == Priority.high:
            if dataset_id not in self.high_access:
                self.high_access.append(dataset_id)
                if dataset_id in self.normal_access:
                    self.normal_access.remove(dataset_id)

        elif prio == Priority.normal:
            if dataset_id not in self.normal_access and dataset_id not in self.high_access:
                self.normal_access.append(dataset_id)

    def _run_queue(self, dataset_id):
        # Create data queue and a storage reading process

        print(self.normal_access)
        data_queue = self.manager.Queue()
        self.storage.read_data(dataset_id, data_queue)

        # Create a list of lists for each operation
        results = []
        while True:
            try:
                for operation in self.operation_table[dataset_id]:
                    results.append([])
                break
            except:
                time.sleep(0.1)

        # Amount of data-blocks
        data_blocks = self.storage.get_size(dataset_id)

        # Amount of operations
        operations = len(self.operation_table[dataset_id])

        # Execute map-operation on the data queue
        for i in range(data_blocks):
            # Fetch data block from data queue
            try:
                data_block = data_queue.get(timeout=3)

                print('- Performing operations on block: ' + str(i) + ', dataset: ' + dataset_id)
                # Perform the operations on fetched data block

#                def reduce_wrapper(): pass
#                reduce_wrapper.__code__ = pickle.loads(reduce_operation)

                op_index = 0
                for operation in self.operation_table[dataset_id]:
                    results[op_index].append(operation.map(data_block))
                    op_index += 1

            except:
                print('! Timeouted waiting for data')

        # Execute the reduce-operation
        op_index = 0
        for operation in self.operation_table[dataset_id]:
            operation.reduce(results[op_index])
            op_index += 1
        self.operation_table[dataset_id] = []

        # Remove the operation meta data for the dataset
        if operations > 0:
            print('!!!!!! SLET')
            del self.operation_table[dataset_id]

    def schedule(self):
        '''
        Schedule the queued operations
        '''
        reading_process = Process(target=self.storage.reader)
        reading_process.start()

        while True:
#            print()
#            print('/ High priority queue is ' + str(self.high_access))
#            print('/ Normal priority queue is ' + str(self.normal_access))
#            print()
            if self.high_access:
                self._pool.apply_async(self._run_queue(self.high_access.pop(0)))
            elif self.normal_access:
                self._pool.apply_async(self._run_queue(self.normal_access.pop(0)))
            else:
                time.sleep(0.5)
