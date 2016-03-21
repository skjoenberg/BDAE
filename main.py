import time
import dis
from multiprocess import Manager, Process, Pool
import dill as pickle
from priorities import Priority
from storage import Storage
from scheduler import Scheduler
from functools import wraps


def print_section(section):
    print('')
    print('&' * (len(section) + 4))
    print('& ' + section + ' &')
    print('&' * (len(section) + 4))
    print('')

# Initiate storage with 3 worker threads
STORAGE = Storage()

# Initiate a scheduler
SCHEDULER = Scheduler(STORAGE, 3)

# Create dataset
DS1_B1 = '1111' * 1024
DS1_B2 = '2222' * 1024
DS1_B3 = '3333' * 1024
DS1_B4 = '4444' * 1024
DS1 = [DS1_B1, DS1_B2, DS1_B3, DS1_B4,
       DS1_B1, DS1_B2, DS1_B3, DS1_B4,
       DS1_B1, DS1_B2, DS1_B3, DS1_B4,
       DS1_B1, DS1_B2, DS1_B3, DS1_B4,
       DS1_B1, DS1_B2, DS1_B3, DS1_B4,
       DS1_B1, DS1_B2, DS1_B3, DS1_B4,
       DS1_B1, DS1_B2, DS1_B3, DS1_B4,
       DS1_B1, DS1_B2, DS1_B3, DS1_B4,
       DS1_B1, DS1_B2, DS1_B3, DS1_B4,
       DS1_B1, DS1_B2, DS1_B3, DS1_B4,
       DS1_B1, DS1_B2, DS1_B3, DS1_B4,
       DS1_B1, DS1_B2, DS1_B3, DS1_B4,
       DS1_B1, DS1_B2, DS1_B3, DS1_B4,
       DS1_B1, DS1_B2, DS1_B3, DS1_B4,
       DS1_B1, DS1_B2, DS1_B3, DS1_B4,
       DS1_B1, DS1_B2, DS1_B3, DS1_B4]

DS2_B1 = 'aaaa' * 1024
DS2_B2 = 'bbbb' * 1024
DS2_B3 = 'cccc' * 1024
DS2_B4 = 'dddd' * 1024
DS2 = [DS2_B1, DS2_B2, DS2_B3, DS2_B4]

# Add the data set to the storage
print_section('Creating datasets')

print('^ Creating dataset: DS1, size: ' + str(len(DS1)))
STORAGE.add_dataset('DS1', DS1, len(DS1))
print()
print('^ Creating  dataset: DS2, size: ' + str(len(DS2)))
STORAGE.add_dataset('DS2', DS2, len(DS2))

start = time.time()

# First map-operation
def length(argument):
    return len(argument)

# First reduce-operation
def sum(arguments):
    res = 0
    for argument in arguments:
        res += argument
    print()
    print('* The size of the dataset is ' + str(res) + ' bytes')

# Second map-operation
def digit(argument):
    return argument.isdigit()

# Second reduce-operation
def digits(arguments):
    for argument in arguments:
        if not argument:
            print('* The dataset does not only contain digits')
            return
    print()
    print('* The dataset only contains digits')

def hue(scheduler, mapper1, reducer1, mapper2, reducer2):
    for i in range(100):
        scheduler.add_operation('DS1', Priority.normal, mapper1, reducer1)
        scheduler.add_operation('DS1', Priority.normal, mapper2, reducer2)
        scheduler.add_operation('DS2', Priority.high, mapper1, reducer1)
        scheduler.add_operation('DS2', Priority.low, mapper2, reducer2)

length_code = length.__code__
sum_code = sum.__code__
digit_code = digit.__code__
digits_code = digits.__code__
map1 = pickle.dumps(length_code)
rec1 = pickle.dumps(sum_code)
map2 = pickle.dumps(digit_code)
rec2 = pickle.dumps(digits_code)

lolle = Process(target=hue, args=(SCHEDULER, map1, rec1, map2, rec2))
lolle.start()
time.sleep(2)
print_section('Scheduling operations')
SCHEDULER.schedule()
