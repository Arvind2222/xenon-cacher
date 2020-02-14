from cacher import Cacher


cacher = Cacher("amqp://guest:guest@localhost/", "mongodb://localhost")
cacher.run()
