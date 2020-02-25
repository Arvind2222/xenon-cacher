import aio_pika
import asyncio
import traceback
import aioredis
import logging
import msgpack

from lua_scripts import scripts


log = logging.getLogger(__name__)


def hash_safe(obj: dict):
    for key, value in obj.items():
        if not isinstance(value, str):
            obj[key] = msgpack.packb(value)


class Cacher:
    def __init__(self, rabbit_url, redis_url, queue="cache", loop=None):
        self.rabbit_url = rabbit_url
        self.redis_url = redis_url
        self.queue = queue

        self.r_con = None
        self.r_channel = None
        self.redis = None

        self._prefix = "cache_"

        self.loop = loop or asyncio.get_event_loop()

    def prefix(self, key):
        return self._prefix + key

    async def _message_received(self, msg):
        payload = msgpack.unpackb(msg.body)
        event, shard_id, data = payload["event"], payload["shard_id"], payload["data"]
        ev = event.lower()
        try:
            func = getattr(self, "cache_" + ev)
        except AttributeError:
            pass

        else:
            await func(shard_id, data)

    async def cache_guild_create(self, _, data):
        ignore = ("emojis", "voice_states", "presences")
        for k in ignore:
            data.pop(k, None)

        guild_id = data["id"]

        roles = data.pop("roles", [])
        for role in roles:
            role_id = role["id"]
            role["guild_id"] = guild_id
            hash_safe(role)
            await self.redis.hmset_dict(self.prefix(f"roles_{role_id}"), role)
            await self.redis.sadd(self.prefix(f"guilds_{guild_id}_roles"), role_id)

        channels = data.pop("channels", [])
        for channel in channels:
            channel_id = channel["id"]
            channel["guild_id"] = guild_id
            hash_safe(channel)
            await self.redis.hmset_dict(self.prefix(f"channels_{channel_id}"), channel)
            await self.redis.sadd(self.prefix(f"guilds_{guild_id}_channels"), channel_id)

        members = data.pop("members", [])
        for member in members:
            user_id = member["user"]["id"]
            member["guild_id"] = guild_id
            hash_safe(member)
            await self.redis.hmset_dict(self.prefix(f"guilds_{guild_id}_members_{user_id}"), member)
            await self.redis.sadd(self.prefix(f"guilds_{guild_id}_members"), user_id)

        hash_safe(data)
        await self.redis.sadd(self.prefix("guilds"), guild_id)
        await self.redis.hmset_dict(self.prefix(f"guilds_{guild_id}"), data)

    def cache_guild_update(self, _, data):
        return self.cache_guild_create(_, data)

    async def cache_guild_delete(self, _, data):
        guild_id = data["id"]
        await self.redis.srem(self.prefix("guilds"), guild_id)
        await self.redis.delete(self.prefix(f"guilds_{guild_id}"))

        channels = await self.redis.smembers(self.prefix(f"guilds_{guild_id}_channels"))
        await self.redis.delete(*[self.prefix(f"channels_{ch}") for ch in channels])

        roles = await self.redis.smembers(self.prefix(f"guilds_{guild_id}_roles"))
        await self.redis.delete(*[self.prefix(f"roles_{r}") for r in roles])

        members = await self.redis.smembers(self.prefix(f"guilds_{guild_id}_members"))
        await self.redis.delete(*[self.prefix(f"guilds_{guild_id}_members_{u}") for u in members])

    async def cache_channel_create(self, _, data):
        guild_id, channel_id = data.get("guild_id"), data["id"]
        hash_safe(data)
        await self.redis.hmset_dict(self.prefix(f"channels_{channel_id}"), data)
        if guild_id is not None:
            await self.redis.sadd(self.prefix(f"guilds_{guild_id}_channels"), channel_id)

        else:
            await self.redis.sadd(self.prefix(f"dm_channels"), channel_id)

    async def cache_channel_update(self, _, data):
        channel_id = data["id"]
        hash_safe(data)
        await self.redis.hmset_dict(self.prefix(f"channels_{channel_id}"), data)

    async def cache_channel_delete(self, _, data):
        guild_id, channel_id = data.get("guild_id"), data["id"]
        await self.redis.delete(self.prefix(f"channels_{channel_id}"))
        if guild_id is not None:
            await self.redis.srem(self.prefix(f"guilds_{guild_id}_channels"), channel_id)

        else:
            await self.redis.srem(self.prefix(f"dm_channels"), channel_id)

    async def cache_guild_role_create(self, _, data):
        role = data["role"]
        guild_id, role_id = data["guild_id"], role["id"]
        role["guild_id"] = guild_id
        hash_safe(role)
        await self.redis.hmset_dict(self.prefix(f"roles_{role_id}"), role)
        await self.redis.sadd(self.prefix(f"guilds_{guild_id}_roles"), role_id)

    async def cache_guild_role_update(self, _, data):
        role = data["role"]
        guild_id, role_id = data["guild_id"], role["id"]
        role["guild_id"] = guild_id
        hash_safe(role)
        await self.redis.hmset_dict(self.prefix(f"roles_{role_id}"), role)

    async def cache_guild_role_delete(self, _, data):
        guild_id, role_id = data["guild_id"], data["role_id"]
        await self.redis.delete(self.prefix(f"roles_{role_id}"))
        await self.redis.srem(self.prefix(f"guilds_{guild_id}_roles"), role_id)

    async def cache_guild_member_add(self, _, data):
        guild_id, user_id = data["guild_id"], data["user"]["id"]
        hash_safe(data)
        await self.redis.hmset_dict(f"guilds_{guild_id}_members_{user_id}", data)
        await self.redis.sadd(f"guilds_{guild_id}_members", user_id)

    async def cache_guild_member_update(self, _, data):
        guild_id, user_id = data["guild_id"], data["user"]["id"]
        hash_safe(data)
        await self.redis.hmset_dict(self.prefix(f"guilds_{guild_id}_members_{user_id}"), data)

    async def cache_guild_member_remove(self, _, data):
        guild_id, user_id = data["guild_id"], data["user"]["id"]
        await self.redis.delete(self.prefix(f"guilds_{guild_id}_members_{user_id}"))
        await self.redis.srem(self.prefix(f"guilds_{guild_id}_members"), user_id)

    async def cache_latency_update(self, shard_id, data):
        await self.redis.hset(self.prefix("latencies"), shard_id, data["latency"])

    async def cache_disconnect(self, shard_id, data):
        await self.redis.hset(self.prefix("latencies"), shard_id, -1)

    async def cache_start(self, _, data):
        await self.redis.set(self.prefix("shard_count"), data.get("shard_count", 1))

    async def start(self):
        try:
            self.redis = await aioredis.create_redis_pool(self.redis_url)
            self.r_con = await aio_pika.connect_robust(self.rabbit_url)
            self.r_channel = await self.r_con.channel()
            await self.r_channel.set_qos()
            queue = await self.r_channel.declare_queue("cache")
            await queue.consume(self._message_received, no_ack=True)

        except ConnectionError:
            traceback.print_exc()
            await asyncio.sleep(5)
            return await self.start()

    def run(self):
        self.loop.create_task(self.start())
        self.loop.run_forever()
