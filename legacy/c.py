def sender(host='127.0.0.1', port=5001):
    pid = os.getpid()

    log(LOG_INFO, "sender_start", "sender")

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))

    log(LOG_NOTIFY, "connected", "sender")

    log(LOG_INFO, "function_reached", "sender")

    while True:
        time.sleep(1)

        # Comment out to simulate hang
        log(LOG_DEBUG, "heartbeat_send", "sender")
        send_json(s, {
            "type": "heartbeat",
            "pid": pid
        })
