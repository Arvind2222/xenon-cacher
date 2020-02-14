import aio_pika
import asyncio
import ujson as json
import traceback
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo


class Cacher:
    def __init__(self, rabbit_url, mongo_url, queue="cache", loop=None):
        self.rabbit_url = rabbit_url
        self.queue = queue

        self.r_con = None
        self.r_channel = None

        self.loop = loop or asyncio.get_event_loop()
        self.db = AsyncIOMotorClient(host=mongo_url).cache

    async def _message_received(self, msg):
        payload = json.loads(msg.body)
        event, data = payload["event"], payload["data"]
        ev = event.lower()
        try:
            func = getattr(self, "cache_" + ev)
        except AttributeError:
            pass

        else:
            await func(data)

    async def cache_guild_create(self, data):
        ignore = ("emojis", "voice_states", "presences")
        for k in ignore:
            data.pop(k, None)

        guild_id = data["id"]

        roles = data.pop("roles", [])
        for role in roles:
            role["_id"] = role["id"]
            role["guild_id"] = guild_id
            await self.db.roles.update_one({"_id": role["id"]}, {"$set": role}, upsert=True)

        channels = data.pop("channels", [])
        for channel in channels:
            channel["_id"] = channel["id"]
            channel["guild_id"] = guild_id
            await self.db.channels.update_one({"_id": channel["id"]}, {"$set": channel}, upsert=True)

        members = data.pop("members", [])
        for member in members:
            member["guild_id"] = guild_id
            await self.db.members.update_one({
                # user.id and guild_id should be a unique compound index
                "id": member["user"]["id"],
                "guild_id": guild_id
            }, {"$set": member}, upsert=True)

        data["_id"] = guild_id
        await self.db.guilds.update_one({"_id": guild_id}, {"$set": data}, upsert=True)

    def cache_guild_update(self, data):
        return self.cache_guild_create(data)

    async def cache_guild_delete(self, data):
        guild_id = data["id"]
        await self.db.guilds.delete_one({"_id": guild_id})
        await self.db.members.delete_many({"guild_id": guild_id})
        await self.db.channels.delete_many({"guild_id": guild_id})
        await self.db.roles.delete_many({"guild_id": guild_id})

    async def cache_channel_create(self, data):
        data["_id"] = data["id"]
        await self.db.channels.update_one({"_id": data["id"]}, {"$set": data}, upsert=True)

    def cache_channel_update(self, data):
        return self.cache_channel_create(data)

    async def cache_channel_delete(self, data):
        await self.db.channels.delete_one({"_id": data["id"]})

    async def cache_guild_role_create(self, data):
        role = data["role"]
        role["_id"] = role["id"]
        role["guild_id"] = data["guild_id"]
        await self.db.roles.update_one({"_id": role["id"]}, {"$set": role}, upsert=True)

    def cache_guild_role_update(self, data):
        return self.cache_guild_role_create(data)

    async def cache_guild_role_delete(self, data):
        await self.db.roles.delete_one({"_id": data["role_id"]})

    async def cache_guild_member_add(self, data):
        await self.db.members.update_one({
            # user.id and guild_id should be a unique compound index
            "id": data["user"]["id"],
            "guild_id": data["guild_id"]
        }, {"$set": data}, upsert=True)

    def cache_guild_member_update(self, data):
        return self.cache_guild_member_add(data)

    async def cache_guild_member_remove(self, data):
        await self.db.members.delete_one({"id": data["user"]["id"], "guild_id": data["guild_id"]})

    async def start(self):
        try:
            self.r_con = await aio_pika.connect_robust(self.rabbit_url)
            self.r_channel = await self.r_con.channel()
            await self.r_channel.set_qos(prefetch_count=100)
            await self.db.members.create_index("guild_id")
            await self.db.roles.create_index("guild_id")
            await self.db.channels.create_index("guild_id")
            await self.db.members.create_index(
                [("guild_id", pymongo.ASCENDING), ("user.id", pymongo.ASCENDING)],
                unique=True
            )

            queue = await self.r_channel.declare_queue("cache", arguments={"x-max-length": 10000})
            async with queue.iterator() as messages:
                async for msg in messages:
                    async with msg.process():
                        await self._message_received(msg)

        except ConnectionError:
            traceback.print_exc()
            await asyncio.sleep(5)
            return await self.start()

    def run(self):
        self.loop.run_until_complete(self.start())
