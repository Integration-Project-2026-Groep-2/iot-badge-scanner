import logging
import os
import sys
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

import cv2
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

cap = None
qcd = cv2.QRCodeDetector()
latest_frame = None
frame_lock = threading.Lock()
running = True
frame_count = 0

HA_HOST = os.environ.get("HA_HOST", "0.0.0.0")
HA_PORT = int(os.environ.get("HA_PORT", "8080"))
API_URL = os.environ.get("API_URL", "http://localhost:3000/checkin")

API_TIMEOUT = 5.0
CAMERA_WIDTH = 640
CAMERA_HEIGHT = 360
CAMERA_FPS = 5
BRIGHTNESS_HIGH = 180
BRIGHTNESS_LOW = 70
DEBUG = True


def send_checkin(badge_id: str) -> bool:
    """Send check-in request to API. Returns True if successful."""
    payload = {
        "id": badge_id,
        "timestamp": datetime.now().isoformat(),
    }
    try:
        response = requests.post(API_URL, json=payload, timeout=API_TIMEOUT)
        if response.status_code == 200:
            logger.info(f"Check-in successful: {badge_id}")
            return True
        else:
            logger.warning(
                f"Check-in failed for {badge_id}: HTTP {response.status_code}"
            )
            return False
    except requests.Timeout:
        logger.error(f"Check-in error for {badge_id}: timeout after {API_TIMEOUT}s")
        return False
    except requests.RequestException as e:
        logger.error(f"Check-in error for {badge_id}: {e}")
        return False


def handle_qr_scan(qr_data: str):
    """Process detected QR code."""
    logger.info(f"QR code detected: {qr_data}")
    if send_checkin(qr_data):
        logger.info("[ACCESS_GRANTED]")
    else:
        logger.warning("[ACCESS_DENIED]")


class StreamHandler(BaseHTTPRequestHandler):
    """HTTP request handler for MJPEG streaming."""

    def do_GET(self):
        """Handle GET requests."""
        if self.path != "/stream.mjpg":
            self.send_response(404)
            self.end_headers()
            return

        self.send_response(200)
        self.send_header("Content-Type", "multipart/x-mixed-replace; boundary=frame")
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

    def log_message(self, format, *args):
        """Suppress default HTTP logging."""
        pass


def start_http_server():
    """Start MJPEG stream server in background thread."""
    server = HTTPServer((HA_HOST, HA_PORT), StreamHandler)
    logger.info(f"Stream server started on http://{HA_HOST}:{HA_PORT}/stream.mjpg")
    server.serve_forever()


def init_camera():
    """Initialize camera with proper settings."""
    global cap

    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        logger.error("Failed to open camera device /dev/video0")
        sys.exit(1)

    # NOTE(nasr): Extra release step to reset camera settings
    cap.release()
    cap = cv2.VideoCapture(0)

    if not cap.isOpened():
        logger.error("Failed to reopen camera after reset")
        sys.exit(1)

    cap.set(cv2.CAP_PROP_FRAME_WIDTH, CAMERA_WIDTH)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, CAMERA_HEIGHT)
    cap.set(cv2.CAP_PROP_FPS, CAMERA_FPS)
    cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

    ret, frame = cap.read()
    if not ret or frame is None:
        logger.error("Camera returned empty frame during initialization")
        cap.release()
        sys.exit(1)

    logger.info(f"Camera initialized: {CAMERA_WIDTH}x{CAMERA_HEIGHT} @ {CAMERA_FPS}fps")

    if DEBUG:
        props = [
            (cv2.CAP_PROP_FRAME_WIDTH, "FRAME_WIDTH"),
            (cv2.CAP_PROP_FRAME_HEIGHT, "FRAME_HEIGHT"),
            (cv2.CAP_PROP_FPS, "FPS"),
            (cv2.CAP_PROP_BRIGHTNESS, "BRIGHTNESS"),
            (cv2.CAP_PROP_CONTRAST, "CONTRAST"),
            (cv2.CAP_PROP_SATURATION, "SATURATION"),
            (cv2.CAP_PROP_EXPOSURE, "EXPOSURE"),
            (cv2.CAP_PROP_GAIN, "GAIN"),
        ]
        logger.debug("Camera properties:")
        for prop, name in props:
            val = cap.get(prop)
            logger.debug(f"  {name}: {val}")


# note(nasr): having an issue with camera brightness handlign.
# when the qr code brightness it too high. we get some issues
# this should scale the brightness in the video loop to adjust to the camera
def adjust_brightness(frame):
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    mean = gray.mean()

    if mean > BRIGHTNESS_HIGH:
        adjusted = cv2.convertScaleAbs(gray, alpha=0.7, beta=-30)
        if DEBUG:
            logger.debug(f"Brightness adjustment: overexposed ({mean:.1f})")
        return adjusted
    elif mean < BRIGHTNESS_LOW:
        adjusted = cv2.convertScaleAbs(gray, alpha=1.3, beta=20)
        if DEBUG:
            logger.debug(f"Brightness adjustment: underexposed ({mean:.1f})")
        return adjusted

    return frame


def detect_qr_codes(frame):
    ret_qr, decoded_info, points, _ = qcd.detectAndDecodeMulti(frame)

    if not ret_qr or not decoded_info:
        return False, [], frame

    annotated = frame.copy()
    for qr_data, qr_points in zip(decoded_info, points):
        color = (0, 255, 0) if qr_data else (0, 0, 255)
        annotated = cv2.polylines(
            annotated,
            [qr_points.astype(int)],
            True,
            color,
            4,
        )

    return True, [data for data in decoded_info if data], annotated


def camera_loop():
    global latest_frame, frame_count, running

    logger.info("Starting camera loop")
    try:
        while running:
            ret, frame = cap.read()
            if not ret:
                logger.warning("Failed to read frame from camera")
                continue

            frame = adjust_brightness(frame)

            found, qr_data_list, annotated_frame = detect_qr_codes(frame)

            if found:
                if DEBUG:
                    logger.debug(f"QR codes detected: {len(qr_data_list)}")
                for qr_data in qr_data_list:
                    handle_qr_scan(qr_data)

            with frame_lock:
                latest_frame = annotated_frame.copy()

            frame_count += 1
            if frame_count % 100 == 0 and DEBUG:
                logger.debug(
                    f"Frame {frame_count}: "
                    f"min={annotated_frame.min()}, "
                    f"max={annotated_frame.max()}, "
                    f"mean={annotated_frame.mean():.2f}"
                )
    except KeyboardInterrupt:
        logger.info("Camera loop interrupted")
    except Exception as e:
        logger.error(f"Camera loop error: {e}", exc_info=True)
    finally:
        running = False
        if cap:
            cap.release()
        logger.info("Camera released")


def main():
    global running

    try:
        logger.info(
            f"Badge Scanner starting: "
            f"camera={CAMERA_WIDTH}x{CAMERA_HEIGHT}@{CAMERA_FPS}fps, "
            f"api_url={API_URL}"
        )

        init_camera()

        http_thread = threading.Thread(target=start_http_server, daemon=True)
        http_thread.start()

        camera_loop()

    except KeyboardInterrupt:
        logger.info("Exiting")
        running = False
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        running = False
        sys.exit(1)


if __name__ == "__main__":
    main()
