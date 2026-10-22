
import psutil
import os 
import time 
import faulthandler
import threading
from multiprocessing import Queue , Process
import traceback
import sys


def receiver(queue):
    print(f"queue.get{queue.get() }")
    events = [] 
    while True:
        msg = queue.get()  
        events.append(msg)
        #TO DO : check if msg repeats in events and how often 
        if msg == "STOP":
            print("Receiver stopping.")
            break
        print(f"Received signal: {msg}")



def watchdog(pid):
    process = psutil.Process(pid)
    while True:
        mem = process.memory_info().rss / (1024 * 1024)
        cpu = process.cpu_percent(interval=1)

        if mem > 500:
            print(f"WARNING: High RAM usage: {mem:.2f} MB")
            for thread_id, frame in sys._current_frames().items():
                print(f"\nThread {thread_id} Stack trace:")
                traceback.print_stack(frame) 
        if cpu > 90:
            print(f"WARNING: High CPU usage: {cpu:.1f}%")
            for thread_id, frame in sys._current_frames().items():
                print(f"\nThread {thread_id} Stack trace:")
                traceback.print_stack(frame) 

        # Optional: dump stack if CPU > 90% or high repetition
#        for thread_id, frame in sys._current_frames().items():
#            print(f"\nThread {thread_id} Stack trace:")
#            traceback.print_stack(frame)

        time.sleep(5)  # adjust interval

def run(pid,queue):


    




    threading.Thread(target=watchdog,args= (pid, ), daemon=True).start()
    
 
    p2 = Process(target=receiver, args=(queue,))
    p2.start()
    process = psutil.Process(pid)
    while True:
        mem = process.memory_info().rss / (1024 * 1024)  
        cpu = process.cpu_percent(interval=1)

        if mem > 500:
            print(f"WARNING: High RAM usage: {mem:.2f} MB")

        if cpu > 90:
            print(f"WARNING: High CPU usage: {cpu:.1f}%")

        time.sleep(1)

