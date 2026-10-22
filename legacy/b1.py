import time
import os
from multiprocessing import Process
import a3
import socket
import json

def send_json(sock, obj):
    sock.sendall(json.dumps(obj).encode())


def sender(host='127.0.0.1', port=5001):
    
    log(LOG_INFO, "sender_start", "sender")
    time.sleep(2)  # simulate work

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        log(LOG_NOTIFY, "connected", "sender")

        #s.sendall(b"Function reached!")
        log(LOG_INFO, "function_reached", "sender")

        while True:
            time.sleep(1)

            # Comment out to simulate hang
            log(LOG_DEBUG, "heartbeat_send", "sender")
            send_json(s, {
                "type": "heartbeat",
                "pid": pid
            })


    except ConnectionRefusedError:
        print("Could not connect to receiver.")
    finally:
        s.close()

if __name__ == "__main__":
    pid = os.getpid()
    print(f"The current process ID is: {pid}")

    # Start the socket-based run (receiver + watchdog)
    p1 = Process(target=a3.run, args=(pid,))
    p1.start()

    # Start the sender process
    p2 = Process(target=sender)
    p2.start()

    p1.join()
    p2.join()
    print("Main process finished.")
