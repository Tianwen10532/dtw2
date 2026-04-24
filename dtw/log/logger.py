import logging
import logging.handlers
import time

LOG_SERVER_IP = "10.156.169.81"
LOG_SERVER_PORT = 9020

logger = logging.getLogger("my_app")
logger.setLevel(logging.INFO)

socket_handler = logging.handlers.SocketHandler(
    LOG_SERVER_IP,
    LOG_SERVER_PORT
)
logger.addHandler(socket_handler)
