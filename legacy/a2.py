import psutil
import os
import time
import faulthandler
import threading
import traceback
import sys
import socket
import json

last_heartbeat = None
HEARTBEAT_TIMEOUT = 5  # seconds

LOG_DEBUG = 10
LOG_INFO = 20
LOG_NOTIFY = 25
LOG_WARNING = 30
LOG_ERROR = 40
LOG_CRITICAL = 50

LOG_FILE = "app.log"
CONSOLE_LEVEL = LOG_WARNING
FILE_LEVEL = LOG_DEBUG

def log(level, event_type, source, message="", data=None, pid=None):
    entry = {
        "ts": time.time(),
        "level": level,
        "event": event_type,
        "source": source,
        "pid": pid or os.getpid(),
        "message": message,
        "data": data or {}
    }

    line = json.dumps(entry)

    # Write to file
    if level >= FILE_LEVEL:
        with open(LOG_FILE, "a", buffering=1) as f:
            f.write(line + "\n")

    # Print warnings+ to console
    if level >= CONSOLE_LEVEL:
        print(line, file=sys.stderr)



        
def json_msg(msg_type, source, message="", data=None, pid=None):
    return json.dumps({
        "type": msg_type,
        "source": source,
        "pid": pid,
        "timestamp": time.time(),
        "message": message,
        "data": data or {}
    }).encode()



def dump_all_threads():
    print("[Watchdog] Dumping stack traces:")
    for thread_id, frame in sys._current_frames().items():
        print(f"\nThread {thread_id} stack trace:")
        traceback.print_stack(frame)



def receiver(host='127.0.0.1', port=5001):
    global last_heartbeat
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(1)
    print(f"[Socket] Receiver listening on {host}:{port}")
    conn, addr = s.accept()
    print(f"[Socket] Connected by {addr}")
    last_heartbeat = time.time()
    events = []

    while True:
        conn.settimeout(1.0)
        try:
            msg = conn.recv(1024).decode()
        except socket.timeout:
            # check heartbeat timeout
            if time.time() - last_heartbeat > HEARTBEAT_TIMEOUT:
                print("[Watchdog] ❌ No heartbeat detected! Possible infinite loop.")
                dump_all_threads()
                break
            continue

        if not msg:
            break

        if msg == "PING":
            last_heartbeat = time.time()
            print("[Socket] Heartbeat received")
            continue

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

