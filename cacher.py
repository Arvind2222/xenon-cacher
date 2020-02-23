import aio_pika
import asyncio
import ujson as json
import traceback
from pymongo import UpdateOne, DeleteOne, DeleteMany
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
import time
import logging


log = logging.getLogger(__name__)


class Cacher:
    def __init__(self, rabbit_url, mongo_url, queue="cache", loop=None):
        self.rabbit_url = rabbit_url
        self.queue = queue

        self.r_con = None
        self.r_channel = None
        self.write_lock = asyncio.Lock()
        self.last_write = time.perf_counter()
        self.max_bulk = 1000
        self.bulk_size = 0
        self.bulk = {}

        self.loop = loop or asyncio.get_event_loop()
        self.db = AsyncIOMotorClient(host=mongo_url).cache

    async def write_bulk(self):
        log.info("Writing %d operations to the database", self.bulk_size)
        self.bulk_size = 0
        self.last_write = time.perf_counter()
        for col, bulk in self.bulk.items():
            operations = [o for _, o in bulk]
            self.bulk[col] = []
            if len(operations) > 0:
                getattr(self.db, col).bulk_write(operations, ordered=False)

    async def write_task(self):
        while True:
            await asyncio.sleep(10)
            if (time.perf_counter() - self.last_write) <= 10:
                # Last write was pretty recent
                continue

            if self.bulk_size > 0:
                await self.write_lock.acquire()
                try:
                    # Check again to not start a unnecessary write
                    if self.bulk_size > 0:
                        await self.write_bulk()

                except:
                    traceback.print_exc()

                finally:
                    self.write_lock.release()

    async def _message_received(self, msg):
        payload = json.loads(msg.body)
        event, shard_id, data = payload["event"], payload["shard_id"], payload["data"]
        ev = event.lower()
        try:
            func = getattr(self, "cache_" + ev)
        except AttributeError:
            pass

        else:
            for col, operation in func(shard_id, data):
                self.bulk_size += 1
                if col not in self.bulk:
                    self.bulk[col] = [(msg, operation)]

                else:
                    self.bulk[col].append((msg, operation))

        if self.bulk_size >= self.max_bulk:
            await self.write_lock.acquire()
            try:
                # Check again to not start a unnecessary write
                if self.bulk_size > self.max_bulk:
                    await self.write_bulk()
            finally:
                self.write_lock.release()

    def cache_guild_create(self, _, data, *, upsert=True):
        ignore = ("emojis", "voice_states", "presences")
        for k in ignore:
            data.pop(k, None)

        guild_id = data["id"]

        roles = data.pop("roles", [])
        for role in roles:
            role["_id"] = role["id"]
            role["guild_id"] = guild_id
            yield "roles", UpdateOne({"_id": role["id"]}, {"$set": role}, upsert=upsert)

        channels = data.pop("channels", [])
        for channel in channels:
            channel["_id"] = channel["id"]
            channel["guild_id"] = guild_id
            yield "channels", UpdateOne({"_id": channel["id"]}, {"$set": channel}, upsert=upsert)

        members = data.pop("members", [])
        for member in members:
            member["guild_id"] = guild_id
            yield "members", UpdateOne({
                # user.id and guild_id should be a unique compound index
                "user.id": member["user"]["id"],
                "guild_id": guild_id
            }, {"$set": member}, upsert=True)

        data["_id"] = guild_id
        yield "guilds", UpdateOne({"_id": guild_id}, {"$set": data}, upsert=True)

    def cache_guild_update(self, _, data):
        yield from self.cache_guild_create(_, data, upsert=False)

    def cache_guild_delete(self, _, data):
        guild_id = data["id"]
        yield "guilds", DeleteOne({"_id": guild_id})
        yield "members", DeleteMany({"guild_id": guild_id})
        yield "channels", DeleteMany({"guild_id": guild_id})
        yield "roles", DeleteMany({"guild_id": guild_id})

    def cache_channel_create(self, _, data, *, upsert=True):
        data["_id"] = data["id"]
        yield "channels", UpdateOne({"_id": data["id"]}, {"$set": data}, upsert=upsert)

    def cache_channel_update(self, _, data):
        yield from self.cache_channel_create(_, data, upsert=False)

    def cache_channel_delete(self, _, data):
        yield "channels", DeleteOne({"_id": data["id"]})

    def cache_guild_role_create(self, _, data, *, upsert=True):
        role = data["role"]
        role["_id"] = role["id"]
        role["guild_id"] = data["guild_id"]
        yield "roles", UpdateOne({"_id": role["id"]}, {"$set": role}, upsert=upsert)

    def cache_guild_role_update(self, _, data):
        yield from self.cache_guild_role_create(_, data, upsert=False)

    def cache_guild_role_delete(self, _, data):
        yield "roles", DeleteOne({"_id": data["role_id"]})

    def cache_guild_member_add(self, _, data):
        yield "members", UpdateOne({
            # user.id and guild_id should be a unique compound index
            "user.id": data["user"]["id"],
            "guild_id": data["guild_id"]
        }, {"$set": data}, upsert=True)

    def cache_guild_member_update(self, _, data):
        yield from self.cache_guild_member_add(_, data)

    def cache_guild_member_remove(self, _, data):
        yield "members", DeleteOne(_, {"user.id": data["user"]["id"], "guild_id": data["guild_id"]})

    def cache_latency_update(self, shard_id, data):
        yield "shards", UpdateOne({"_id": "shard_id"}, {"$set": {
            "_id": shard_id,
            "latency": data["latency"]
        }}, upsert=True)

    def cache_disconnect(self, shard_id, data):
        yield "shards", UpdateOne({"_id": shard_id}, {"$set": {
            "_id": shard_id,
            "latency": -1
        }}, upsert=True)

    async def start(self):
        try:
            self.r_con = await aio_pika.connect_robust(self.rabbit_url)
            self.r_channel = await self.r_con.channel()
            await self.r_channel.set_qos(prefetch_count=self.max_bulk)
            await self.db.roles.create_index("guild_id")
            await self.db.channels.create_index("guild_id")
            await self.db.guilds.create_index("owner_id")
            await self.db.members.create_index(
                [("guild_id", pymongo.ASCENDING), ("user.id", pymongo.ASCENDING)],
                unique=True
            )

            queue = await self.r_channel.declare_queue("cache")
            await queue.consume(self._message_received, no_ack=True)
            self.loop.create_task(self.write_task())

        except ConnectionError:
            traceback.print_exc()
            await asyncio.sleep(5)
            return await self.start()

    def run(self):
        self.loop.create_task(self.start())
        self.loop.run_forever()
