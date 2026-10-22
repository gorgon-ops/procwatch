import time
import os 
from multiprocessing import Queue , Process
import a

def sender(queue):
    print("Sender started.")

    my_function(queue)

    queue.put("STOP") 

def my_function(queue):
    print("Sender reached my_function, sending signal...")
    time.sleep(2)
    queue.put("Function reached!")


if __name__ == "__main__":
    pid = os.getpid()
    q = Queue()
    print(f"The current process ID is: {pid}")
    p1 = Process(target=a.run, args=(pid,q))
    p1.start()

    
    p2 = Process(target=sender, args=(q,))
    p2.start()
    p1.join()
    p2.join()
    time.sleep(1000000)


