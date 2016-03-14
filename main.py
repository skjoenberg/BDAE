from time import sleep
from priorities import Priority
from storage import Storage
from scheduler import Scheduler
import multiprocessing as mp
import pickle

mp.set_start_method('fork')
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
       DS1_B1, DS1_B2, DS1_B3, DS1_B4]

DS2_B1 = 'aaaa' * 1024
DS2_B2 = 'bbbb' * 1024
DS2_B3 = 'cccc' * 1024
DS2_B4 = 'dddd' * 1024
DS2 = [DS2_B1, DS2_B2, DS2_B3, DS2_B4]

# Add the data set to the storage
print('')
print('&&&&&&&&&&&&&&&&&&&&&')
print('& Creating datasets &')
print('&&&&&&&&&&&&&&&&&&&&&')
print('')
STORAGE.add_dataset('DS1', DS1, len(DS1))
STORAGE.add_dataset('DS2', DS2, len(DS2))
STORAGE.add_dataset('DS3', DS2, len(DS2))

def length(argument):
    return len(argument)

def digit(argument):
    return argument.isdigit()

def sum(arguments):
    res = 0
    for argument in arguments:
        res += argument
    print('* The length of the dataset is ' + str(res) + ' bytes')


def digits(arguments):
    for argument in arguments:
        if not argument:
            print('* The dataset is not only digits')
            return
    print('* The dataset is only digits')


print('')
print('&&&&&&&&&&&&&&&&&&&&&')
print('& Adding operations &')
print('&&&&&&&&&&&&&&&&&&&&&')
print('')
SCHEDULER.add_operation('DS1', Priority.normal, length, sum)
SCHEDULER.add_operation('DS2', Priority.high, length, sum)
SCHEDULER.add_operation('DS1', Priority.normal, digit, digits)
SCHEDULER.add_operation('DS2', Priority.low, digit, digits)

print('')
print('&&&&&&&&&&&&&&&&&&&&&&&&&')
print('& Scheduling operations &')
print('&&&&&&&&&&&&&&&&&&&&&&&&&')
print('')
SCHEDULER.schedule()
