# i dont know how to properly implement everything yet but the idea is that we can with opencv
# and than display that information to a home assistant dashboard
import logger
import qrcode
import cv2
# used for rabbitmq
import pika
# used for env variables
import os
import sys
import sqlite3


# init sqlite database and save the temporary users
con = sqlite3.connect('users.db')



# boolean value to check if the access mode is enabled
# True = access/badge verification mode
# False = payment mode
ACCESS_MODE = True  # default to access mode

def load_config():
    try:
        config = {
            'rq_host': os.environ['RABBITMQ_HOST'],
            'rq_user': os.environ['RABBITMQ_USER'],
            'rq_pass': os.environ['RABBITMQ_PASS'],
            'rq_port': int(os.environ['RABBITMQ_PORT']),
            'ha_host': os.environ['HOME_ASSISTANT_HOST'],
            'ha_port': int(os.environ['HOME_ASSISTANT_PORT']),
            'ha_token': os.environ['HOME_ASSISTANT_TOKEN'],
        }
        return config
    except KeyError as e:
        logger.error(f"Missing environment variable: {e}")
        sys.exit(1)

config = load_config()

# RabbitMQ connection
try:
    credentials = pika.PlainCredentials(config['rq_user'], config['rq_pass'])
    parameters = pika.ConnectionParameters(
        config['rq_host'],
        config['rq_port'],
        '/',
        credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    logger.info("Connected to RabbitMQ")
except pika.exceptions.PikaException as e:
    logger.error(f"Failed to connect to RabbitMQ: {e}")
    sys.exit(1)

# Home Assistant setup
ha_url = f"http://{config['ha_host']}:{config['ha_port']}"
try:
    ha_client = Client(ha_url, config['ha_token'])
    entities = ha_client.get_entitites()
    logger.info(f"Connected to Home Assistant. Found {len(entities)} entities")
except Exception as e:
    logger.error(f"Failed to connect to Home Assistant: {e}")

camera_id = 0
delay = 1
window_name = 'Desiderius Festival Badge Scanner'

try:
    # the opencv qr code detectror
    qcd = cv2.QRCodeDetector()
    cap = cv2.VideoCapture(camera_id)
    if not cap.isOpened():
        logger.error("Failed to open camera")
        sys.exit(1)
    logger.info("Camera initialized successfully")
except Exception as e:
    logger.error(f"Camera initialization error: {e}")
    sys.exit(1)


# straight forward becaus the information is already decoded in the qr code.
# im taking the guess that we are decoding the uuid of the user and thats it
# the rest of the informmation are things we can check at run time
def parse_uuid_from_qr_code(qr_data: str): -> uuid.UUID | None:
    try:
        return uuid.UUID(qr_data.strip())
    expect: ValueError
        return None


def authenticate_user(badge_id: str) -> bool:
    """
    TODO(nasr): implement local SQLite cache for offline auth
    TODO(nasr): integrate with Home Assistant or RabbitMQ lookup
    """
    logger.info(f"Authenticating user with badge: {badge_id}")
    try:
        # TODO: Query Home Assistant or local DB for badge validity
        # For now, just log and return True
        return True
    except Exception as e:
        logger.error(f"Authentication failed for {badge_id}: {e}")
        return False

def make_payment(badge_id: str, amount: float) -> bool:
    """
    TODO(nasr): integrate with payment gateway
    TODO(nasr): publish to RabbitMQ payment queue
    """
    if not authenticate_user(badge_id):
        logger.warning(f"Payment blocked: user {badge_id} not authenticated")
        return False

    logger.info(f"Processing payment: {badge_id} - €{amount}")
    try:
        # TODO: Call payment API or publish to RabbitMQ
        return True
    except Exception as e:
        logger.error(f"Payment failed for {badge_id}: {e}")
        return False

def handle_qr_scan(qr_data: str) -> None:
    logger.info(f"QR scan detected: {qr_data}")

    if ACCESS_MODE:
        success = authenticate_user(qr_data)
        status = "ACCESS_GRANTED" if success else "ACCESS_DENIED"
        logger.info(f"Access mode: {status}")
        # TODO: Publish to RabbitMQ access queue
        # channel.basic_publish(exchange='', routing_key='access', body=qr_data)
    else:
        # Payment mode
        make_payment(qr_data, amount=10.0)  # TODO: extract amount from QR or config
        # TODO: Publish to RabbitMQ payment queue

def main() -> int:
    global ACCESS_MODE

    # Parse command-line arguments
    if len(sys.argv) >= 2:
        if sys.argv[1] == '--access-mode':
            ACCESS_MODE = True
            logger.info("Mode: ACCESS (badge verification)")
        elif sys.argv[1] == '--payment-mode':
            ACCESS_MODE = False
            logger.info("Mode: PAYMENT")
        elif sys.argv[1] == '--help':
            # cool comment he. ai generated
            print("""
Badge Scanner for Desiderius Festival

Usage:
  python main.py [--access-mode | --payment-mode]

Modes:
  --access-mode   Verify badge access (default)
  --payment-mode  Process payments

Default: access-mode

Controls:
  'q' key - quit application
            """)
            return 0
        else:
            logger.warning(f"Unknown argument: {sys.argv[1]}")

    logger.info("Starting badge scanner main loop...")

    try:
        # QR code detection loop because we are scanning in video mode
        # we run  a prebuilt detection loop that checks for qr codes.
        # this is hardware intensive. but unless we can get a bar code scanner this is the thing we do
        while True:
            ret, frame = cap.read()
            if not ret:
                logger.warning("Failed to read frame from camera")
                continue

            # Detect QR codes
            ret_qr, decoded_info, points, _ = qcd.detectAndDecodeMulti(frame)

            if ret_qr:
                for qr_data, qr_points in zip(decoded_info, points):
                    if qr_data:
                        logger.info(f"Valid QR detected: {qr_data}")
                        handle_qr_scan(qr_data)
                        color = (0, 255, 0)  # Green
                    else:
                        logger.warning("Invalid QR code")
                        color = (0, 0, 255)  # Red

                    frame = cv2.polylines(frame, [qr_points.astype(int)], True, color, 8)

            cv2.imshow(window_name, frame)

            # Check for quit key ('q')
            if cv2.waitKey(delay) & 0xFF == ord('q'):
                logger.info("User quit (q key pressed)")
                break

        return 0

    except KeyboardInterrupt:
        logger.info("Interrupted by user (Ctrl+C)")
        return 0
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
        return 1

    finally:
        logger.info("Cleaning up...")
        cv2.destroyAllWindows()
        cap.release()
        if connection and not connection.is_closed():
            connection.close()
        logger.info("Shutdown complete")


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
