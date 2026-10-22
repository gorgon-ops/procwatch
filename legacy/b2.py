import time
import os
from multiprocessing import Process
import socket
import a4  

def send_json(sock, obj):
    """Send a JSON message over a socket."""
    sock.sendall(a4.json_msg("heartbeat", "sender", data=obj))

def sender(host='127.0.0.1', port=5001, pid=None):
    a4.log(a4.LOG_INFO, "sender_start", "sender")
    time.sleep(2)  # simulate work

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        a4.log(a4.LOG_NOTIFY, "connected", "sender")

        a4.log(a4.LOG_INFO, "function_reached", "sender")

        while True:
            time.sleep(1)
            a4.log(a4.LOG_DEBUG, "heartbeat_send", "sender")
            send_json(s, {"pid": pid})

    except ConnectionRefusedError:
        print("Could not connect to receiver.")
    finally:
        s.close()

if __name__ == "__main__":
    pid = os.getpid()
    print(f"The current process ID is: {pid}")

    # Start the socket-based run (receiver + watchdog)
    p1 = Process(target=a4.run, args=(pid,))
    p1.start()

    # Start the sender process
    p2 = Process(target=sender, args=("127.0.0.1", 5001, pid))
    p2.start()

    p1.join()
    p2.join()
    print("Main process finished.")
