import psutil
import os
import time
import faulthandler
import threading
import traceback
import sys
import socket

def receiver(host='127.0.0.1', port=5001):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(1)
    print(f"[Socket] Receiver listening on {host}:{port}")
    conn, addr = s.accept()
    print(f"[Socket] Connected by {addr}")

    events = []

    while True:
        msg = conn.recv(1024).decode()
        if not msg:
            break
        events.append(msg)
        if msg == "STOP":
            print("[Socket] Receiver stopping.")
            break
        print(f"[Socket] Received signal: {msg}")

    conn.close()
    s.close()

def watchdog(pid):
    process = psutil.Process(pid)
    while True:
        mem = process.memory_info().rss / (1024 * 1024)
        cpu = process.cpu_percent(interval=1)

        if mem > 500:
            print(f"[Watchdog] WARNING: High RAM usage: {mem:.2f} MB")
            for thread_id, frame in sys._current_frames().items():
                print(f"\nThread {thread_id} Stack trace:")
                traceback.print_stack(frame)

        if cpu > 90:
            print(f"[Watchdog] WARNING: High CPU usage: {cpu:.1f}%")
            for thread_id, frame in sys._current_frames().items():
                print(f"\nThread {thread_id} Stack trace:")
                traceback.print_stack(frame)

        time.sleep(5)

def run(pid):
    # Start watchdog thread
    threading.Thread(target=watchdog, args=(pid,), daemon=True).start()

    # Start receiver process
    from multiprocessing import Process
    p = Process(target=receiver)
    p.start()

