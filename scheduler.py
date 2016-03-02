from multiprocessing import Manager
from operation import Operation
from priorities import Priority


class Scheduler(object):
    '''
    Scheduler
    '''
    def __init__(self, storage):
        # Manager for concurrency
        self.manager = Manager()

        # System storage
        self.storage = storage

        # Queues
        self.high_access = self.manager.list([])
        self.normal_access = self.manager.list([])


    def add_operation(self, dataset_id, prio, operation):
        '''
        Add operation to a dataset
        '''
        # Add to storage
        self.storage.add_operation(operation)

        # Add data block to scheduler
        if prio == Priority.high:
            if dataset_id not in self.high_access:
                self.high_access.append(dataset_id)
        elif prio == Priority.normal:
            if dataset_id not in self.normal_access:
                self.normal_access.append(dataset_id)


    def schedule(self):
        '''
        Schedule the queued operations
        '''
        if self.high_access:
            self.storage.run_queue(self.high_access.pop(0))
        elif self.normal_access:
            self.storage.run_queue(self.normal_access.pop(0))
