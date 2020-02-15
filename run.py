from cacher import Cacher
from os import environ as env
import logging
import sys


logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))


cacher = Cacher(
    env.get("RABBIT_URL") or "amqp://guest:guest@localhost/",
    env.get("MONGO_URL") or "mongodb://localhost"
)
cacher.run()
