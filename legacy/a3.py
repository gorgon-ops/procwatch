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

METRICS_FILE = "metrics.json"
METRICS_HISTORY = "metrics_history.log"

metrics = {
    "heartbeats_received": 0,
    "heartbeats_missed": 0,
    "hangs_detected": 0,
    "high_cpu_events": 0,
    "high_memory_events": 0,
    "uptime_start": time.time()
}

def append_metrics_snapshot():
    snapshot = {
        "ts": time.time(),
        **metrics
    }
    with open(METRICS_HISTORY, "a") as f:
        f.write(json.dumps(snapshot) + "\n")
        
def flush_metrics(interval=10):
    while True:
        time.sleep(interval)
        snapshot = metrics.copy()
        snapshot["uptime_seconds"] = time.time() - metrics["uptime_start"]

        with open(METRICS_FILE, "w") as f:
            json.dump(snapshot, f, indent=2)

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

def collect_stack_traces():
    stacks = {}
    for tid, frame in sys._current_frames().items():
        stacks[str(tid)] = traceback.format_stack(frame)
    return stacks

def log_hang_detected():
    stacks = collect_stack_traces()

    log(
        LOG_WARNING,
        "hang_detected",
        "watchdog",
        message="No heartbeat detected",
        data={"threads": stacks}
    )
def inc(metric, value=1):
    metrics[metric] += value


        
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

    log(LOG_INFO, "socket_listen", "receiver",
        message=f"Listening on {host}:{port}")

    conn, addr = s.accept()
    last_heartbeat = time.time()

    log(LOG_NOTIFY, "socket_connect", "receiver",
        data={"client": addr})

    conn.settimeout(1.0)

    while True:
        try:
            raw = conn.recv(4096)
        except socket.timeout:
            if time.time() - last_heartbeat > HEARTBEAT_TIMEOUT:
                log_hang_detected()
                break
            continue

        if not raw:
            break

        msg = json.loads(raw.decode())
        msg_type = msg.get("type")

        if msg_type == "heartbeat":
            last_heartbeat = time.time()
            log(LOG_DEBUG, "heartbeat", "receiver")
            continue

        if msg_type == "stop":
            log(LOG_INFO, "receiver_stop", "receiver")
            break

        log(LOG_INFO, "message_received", "receiver", data=msg)

    conn.close()
    s.close()

def watchdog(pid):
    process = psutil.Process(pid)
    while True:
        mem = process.memory_info().rss / (1024 * 1024)
        cpu = process.cpu_percent(interval=1)

        if cpu > 90:
            log(
                LOG_WARNING,
                "high_cpu",
                "watchdog",
                message="CPU usage high",
                data={"cpu": cpu}
            )

        if mem > 500:
            log(
                LOG_WARNING,
                "high_memory",
                "watchdog",
                message="Memory usage high",
                data={"mb": mem}
            )


        time.sleep(5)

def run(pid):
    # Start watchdog thread
    threading.Thread(target=watchdog, args=(pid,), daemon=True).start()
    threading.Thread(target=flush_metrics, daemon=True).start()
    # Start receiver process
    from multiprocessing import Process
    p = Process(target=receiver)
    p.start()

