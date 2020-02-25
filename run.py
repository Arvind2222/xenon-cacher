from redis import Cacher
from os import environ as env
import logging
import sys


logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))


cacher = Cacher(
    env.get("RABBIT_URL") or "amqp://guest:guest@localhost/",
    env.get("REDIS_URL") or "redis://localhost"
)
cacher.run()
