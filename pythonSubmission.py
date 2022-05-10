#system libraries
import sys
import time
import psutil
import subprocess
from memory_profiler import profile


#multiprocessing libraries 
from contextlib import contextmanager
from multiprocessing import Manager, Pool


#to run, type in python pythonSubmission.py arg
#where arg is whatever even number of processes you want to spawn


#https://www.geeksforgeeks.org/merge-sort/
def merge_sort(array):
    array_length = len(array)

    if array_length <= 1:
        return array
    #finding middle of array
    middle_index = int(array_length // 2)
    
    
    left = array[0:middle_index]
    right = array[middle_index:]
    
    #splitting array elements into two halves recursively 
    left = merge_sort(left)
    right = merge_sort(right)
    
    return merge(left, right)


def merge(left, right):
    sorted_list = []

    #tuple works better with multiprocessing.
    
    left = left[:]
    right = right[:]


    while len(left) > 0 or len(right) > 0:
        
        if len(left) > 0 and len(right) > 0:
            
            if left[0] <= right[0]:
                sorted_list.append(left.pop(0))
                
            else:
                sorted_list.append(right.pop(0))
                
        elif len(left) > 0:
            sorted_list.append(left.pop(0))
            
        elif len(right) > 0:
            sorted_list.append(right.pop(0))
            
    return sorted_list


#these two merges the arrays 
def merge_sort_multiple(results, array):
  results.append(merge_sort(array))


def merge_multiple(results, array_part_left, array_part_right):
  results.append(merge(array_part_left, array_part_right))

@contextmanager

def process_pool(size):
    #Create a process pool and a block until all processes have completed.

    pool = Pool(size)
    yield pool
    pool.close()
    pool.join()

#decorator function to see whats going on line by line
#@profile
def merge_parallel(array, process_count):

    #splits the merge sort into equal number of proccesses/cores 
    split = int(len(array) / process_count)

    #helps avoid using shared state that python naturally has because of its GIL locks,
    manager = Manager()
    results = manager.list()
    

     #splits the processes up into the number of cores
    with process_pool(size=process_count) as pool:
        for n in range(process_count):
            #create a new process object using the input as a sublist
 
            if n < process_count - 1:
                chunk = array[n * split:(n + 1) * split]
            else:
                # Get the remaining elements in the list
                chunk = array[n * split:]
                
            #performs merge sort on each of the number of processes
            pool.apply_async(merge_sort_multiple, (results, chunk))


    #all processes are now sorted and now we need to merge to one main process
    while len(results) > 1:
        
        with process_pool(size=process_count) as pool:
            #if number of processes are odd, then we pop it off and hold it until we have 2 processes 
            #using multiprocessing again to merge the sublists in parallel
            pool.apply_async(merge_multiple, (results, results.pop(0), results.pop(0)) )


    return results[0]


def command_line_arguments():
    #Makes it easier to just type in the number of processes that you want to create in command line
    #instead of manually calling it in main
    if len(sys.argv) > 1:
        
        #takes in the argument as an int
        total_processes = int(sys.argv[1])
        
        if total_processes > 1:
            #process count should be an even number to make it easier for the cores to do work
            if total_processes % 2 != 0:
                print('Process count should be an even number.')
                sys.exit(1)
        print('Using %i cores' %total_processes)
    else:
        total_processes = 1

    return {'process_count': total_processes}



#need to do this to safeguard the the script   
if __name__ == '__main__':
    parameters = command_line_arguments()

    process_count = parameters['process_count']


    #read in numbers of 100k and 1 million
    randomNumbersArray = []    
    file = open("rand1mil.txt", "r").read()
    #file = open("random numbers", "r").read()
    file = file.split(",")
    for num in file:
        randomNumbersArray.append(num)


    #starting the concurrent process
    
    start = time.time()

    print("Starting normal unparallel merge sort")
    single = merge_sort(randomNumbersArray)
    print("Timer for normal sorted unparallel merge", time.time()-start)

    # Create a copy first and rerun it to see the difference in times between normal merge and multiple
    randomNumbersArray_sorted = randomNumbersArray[:]
    randomNumbersArray_sorted.sort()


    print('\nStarting parallel sort.')
    process_start = time.process_time()
    start_parallel = time.time()
    
    parallel_sorted_list =  merge_parallel(randomNumbersArray, process_count)

    process_end = time.process_time()
    end_parallel = time.time()

    process_time = process_end - process_start
    
    print("Are both single merge sort and concurrent sort equal?", single == parallel_sorted_list)
    #PID stuff
    DETACHED_PROCESS = 0x00000008

    #for windows processes
    #this prints the final process when all of them merge together 
    pid = subprocess.Popen([sys.executable, "OS Pseudocode.py"], creationflags=DETACHED_PROCESS).pid
    
    #for FreeBSD and linux https://stackoverflow.com/questions/89228/how-do-i-execute-a-program-or-call-a-system-command
    #When parent finishes, child apparently is done too, so to prevent that, we have this 
#   pid = subprocess.Popen([sys.executable, "OS Pseudocode.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)

    
    #stats
    print("\nElapsed Execution time", end_parallel - start_parallel)
    print("Process CPU Time", process_time)
    print("CPU Percentage of PID", psutil.Process(pid).cpu_percent(),"%") 
    print("Total CPU Percentage", psutil.cpu_percent(),"%") 
    print("Memory Maps", psutil.Process(pid).memory_maps())
    print("Memory info", psutil.Process(pid).memory_info())
    print("Total Memory used on whole CPU", psutil.virtual_memory()[3]) #pulls up used memory
    print("PID", pid)
     
    context_switch, interrupt, soft_interrupt, system_calls = psutil.cpu_stats()
    current_cpu_freq, min_cpu_freq, max_cpu_freq = psutil.cpu_freq()
     
    print("Context Switches for %i" %pid, psutil.Process( pid ).num_ctx_switches())
    print("Interrupts since boot", interrupt)
    print("Number of system calls since boot", system_calls)
    
    #this has to get commetned out during linux/bsd runs since cpu_freq doesnt work there
    print("Current CPU Frequency", current_cpu_freq)
    print("Minimum CPU Frequency", min_cpu_freq)
    print("Maximum CPU Frequency", max_cpu_freq)


