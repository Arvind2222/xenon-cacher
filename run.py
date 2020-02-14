from cacher import Cacher
from os import environ as env


cacher = Cacher(
    env.get("RABBIT_URL") or "amqp://guest:guest@localhost/",
    env.get("MONGO_URL") or "mongodb://localhost"
)
cacher.run()
