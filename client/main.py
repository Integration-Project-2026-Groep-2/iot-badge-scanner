import json
import os
import sys
import threading
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

import cv2
import requests

ACCESS_MODE = True

latest_frame = None
frame_lock = threading.Lock()


def load_config():
    return {
        "ha_host": os.environ.get("HA_HOST", "0.0.0.0"),
        "ha_port": int(os.environ.get("HA_PORT", "8080")),
        "api_url": os.environ.get("API_URL", "http://localhost:3000/checkin"),
    }


config = load_config()

qcd = cv2.QRCodeDetector()

cap = cv2.VideoCapture(0)

if not cap.isOpened():
    print("[ERROR] Failed to open camera")
    sys.exit(1)

cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1280 / 4)
cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720 / 4)
cap.set(cv2.CAP_PROP_FPS, 60)
cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)


def send_checkin(badge_id: str) -> bool:
    payload = {
        "id": badge_id,
        "timestamp": datetime.now().isoformat(),
    }

    try:
        response = requests.post(
            config["api_url"],
            json=payload,
            timeout=5,
        )

        return response.status_code == 200

    except requests.RequestException as e:
        print(f"[API] Error: {e}")
        return False


def authenticate_user(badge_id: str) -> bool:
    return send_checkin(badge_id)


def handle_qr_scan(qr_data: str):
    print(f"[QR] {qr_data}")

    success = authenticate_user(qr_data)

    if success:
        print("[ACCESS_GRANTED]")
    else:
        print("[ACCESS_DENIED]")


class StreamHandler(BaseHTTPRequestHandler):
    def do_GET(self):

        if self.path != "/stream.mjpg":
            self.send_response(404)
            self.end_headers()
            return

        self.send_response(200)

        self.send_header("Content-type", "multipart/x-mixed-replace; boundary=frame")

        self.end_headers()

        try:
            while True:
                with frame_lock:
                    frame = latest_frame

                if frame is None:
                    continue

                success, jpeg = cv2.imencode(".jpg", frame)

                if not success:
                    continue

                self.wfile.write(b"--frame\r\n")

                self.send_header("Content-Type", "image/jpeg")

                self.send_header("Content-Length", str(len(jpeg)))

                self.end_headers()

                self.wfile.write(jpeg.tobytes())
                self.wfile.write(b"\r\n")

        except BrokenPipeError:
            pass


def start_http_server():

    server = HTTPServer(
        (config["ha_host"], config["ha_port"]),
        StreamHandler,
    )

    print(
        f"[HTTP] Stream running on "
        f"http://{config['ha_host']}:{config['ha_port']}/stream.mjpg"
    )

    server.serve_forever()


def camera_loop():

    global latest_frame

    while True:
        ret, frame = cap.read()

        if not ret:
            continue

        ret_qr, decoded_info, points, _ = qcd.detectAndDecodeMulti(frame)

        if ret_qr and decoded_info:
            for qr_data, qr_points in zip(decoded_info, points):
                if qr_data:
                    handle_qr_scan(qr_data)

                    color = (0, 255, 0)

                else:
                    color = (0, 0, 255)

                frame = cv2.polylines(
                    frame,
                    [qr_points.astype(int)],
                    True,
                    color,
                    4,
                )

        with frame_lock:
            latest_frame = frame.copy()


def main():

    http_thread = threading.Thread(
        target=start_http_server,
        daemon=True,
    )

    http_thread.start()

    camera_loop()


if __name__ == "__main__":
    main()
