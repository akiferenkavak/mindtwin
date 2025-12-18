import socket, json, time, random
from datetime import datetime

HOST = "127.0.0.1"
PORT = 8766

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((HOST, PORT))

frame = 0
JOINTS = 6

while True:
    ideal = [1.0] * JOINTS
    actual = [i + random.uniform(-0.5, 0.5) for i in ideal]

    pkt = {
        "frame_no": frame,
        "timestamp": datetime.now().isoformat(),
        "torque_ideal": ideal,
        "torque_actual": actual,
    }

    sock.sendall((json.dumps(pkt) + "\n").encode())
    frame += 1
    time.sleep(0.5)
