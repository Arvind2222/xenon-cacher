import aio_pika
import asyncio
import traceback
import aioredis
import logging
import msgpack

log = logging.getLogger(__name__)

PAIR_SCRIPT = lambda i, o: """
local %s = {}
for key, value in pairs(%s) do
    table.insert(%s, key)
    table.insert(%s, cmsgpack.pack(value))
end
""" % (o, i, o, o)

SCRIPTS = dict(
    start=f"""
    local data = cmsgpack.unpack(ARGV[1])
    {PAIR_SCRIPT('data', 'paired')}
    redis.call('hmset', 'state', unpack(paired))
    """,

    latency_update=f"""
    redis.call('hmset', 'shards', ARGV[2], ARGV[1])
    """,

    disconnect=f"""
    redis.call('hdel', 'latencies', ARGV[2])
    """,

    guild_create=f"""
local data = cmsgpack.unpack(ARGV[1])

data.emojis = nil
data.voice_states = nil
data.presences = nil

if data.roles ~= nil then
    for i, role in pairs(data.roles) do
        role.guild_id = data.id
        redis.call('hset', 'roles', role.id, cmsgpack.pack(role))
        redis.call('sadd', 'guilds:' .. role.guild_id .. ':roles', role.id)
    end
end

data.roles = nil

if data.channels ~= nil then
    for i, channel in pairs(data.channels) do
        channel.guild_id = data.id
        redis.call('hset', 'channels', channel.id, cmsgpack.pack(channel))
        redis.call('sadd', 'guilds:' .. channel.guild_id .. ':channels', channel.id)
    end
end

data.channels = nil

if data.members ~= nil then
    for i, member in pairs(data.members) do
        member.guild_id = data.id
        redis.call('hset', 'guilds:' .. member.guild_id .. ':members', member.user.id, cmsgpack.pack(member))
    end
end

data.members = nil

redis.call('hmset', 'guilds', data.id, cmsgpack.pack(data))

return 1
    """,

    guild_delete="""
local data = cmsgpack.unpack(ARGV[1])
redis.call('hdel', 'guilds', data.id)

local channels = redis.call('smembers', 'guilds:' .. data.id .. ':channels')
redis.call('hdel', 'channels', unpack(channels))
redis.call('del', 'guilds:' .. data.id .. ':channels')

local roles = redis.call('smembers', 'guilds:' .. data.id .. ':roles')
redis.call('hdel', 'roles', unpack(roles))
redis.call('del', 'guilds:' .. data.id .. ':roles')

redis.call('del', 'guilds:' .. data.id .. ':members')
    """,

    channel_create=f"""
local data = cmsgpack.unpack(ARGV[1])

redis.call('hset', 'channels', data.id, ARGV[1])
if data.guild_id ~= nil then
    redis.call('sadd', 'guilds:' .. data.guild_id .. ':channels', data.id)
end

return 1
    """,

    channel_update=f"""
local data = cmsgpack.unpack(ARGV[1])

redis.call('hset', 'channels', data.id, ARGV[1])

return 1
    """,

    channel_delete=f"""
local data = cmsgpack.unpack(ARGV[1])
redis.call('hdel', 'channels', data.id)

if data.guild_id ~= nil then
    redis.call('srem', 'guilds:' .. data.guild_id .. ':channels', data.id)
end

return 1
    """,

    guild_role_create=f"""
local data = cmsgpack.unpack(ARGV[1])
local role = data.role
role.guild_id = data.guild_id

redis.call('hset', 'roles', role.id, cmsgpack.pack(role))
redis.call('sadd', 'guilds:' .. data.guild_id .. ':roles', role.id)

return 1
""",

    guild_role_update=f"""
local data = cmsgpack.unpack(ARGV[1])
local role = data.role
role.guild_id = data.guild_id

redis.call('hset', 'roles', role.id, cmsgpack.pack(role))

return 1
""",

    guild_role_delete=f"""
local data = cmsgpack.unpack(ARGV[1])
redis.call('hdel', 'roles', data.role_id)
redis.call('srem', 'guilds:' .. data.guild_id .. ':roles', data.role_id)

return 1
""",

    guild_member_add=f"""
local data = cmsgpack.unpack(ARGV[1])

redis.call('hset', 'guilds:' .. data.guild_id .. ':members:', data.user.id, ARGV[1])

return 1
""",

    guild_member_update=f"""
local data = cmsgpack.unpack(ARGV[1])

redis.call('hset', 'guilds:' .. data.guild_id .. ':members', data.user.id, ARGV[1])

return 1
""",

    guild_member_remove=f"""
local data = cmsgpack.unpack(ARGV[1])

redis.call('hdel', 'guilds:' .. data.guild_id .. ':members', data.user.id)

return 1
"""
)

SCRIPTS["guild_update"] = SCRIPTS["guild_create"]


# print("\n".join(f"{i + 1} {l}" for i, l in enumerate(SCRIPTS["guild_create"].splitlines())))


class Cacher:
    def __init__(self, rabbit_url, redis_url, queue="cache", loop=None):
        self.rabbit_url = rabbit_url
        self.redis_url = redis_url
        self.queue = queue

        self.r_con = None
        self.r_channel = None
        self.redis = None

        self._prefix = ""

        self.loop = loop or asyncio.get_event_loop()
        self.scripts = {}

    async def run_script(self, name, data, *args):
        script_sha = self.scripts.get(name)
        if script_sha is None:
            return

        await self.redis.evalsha(script_sha, args=[msgpack.packb(data), *args])

    async def _message_received(self, msg):
        payload = msgpack.unpackb(msg.body)
        event, shard_id, data = payload["event"], payload["shard_id"], payload["data"]
        ev = event.lower()
        await self.run_script(ev, data, shard_id)

    async def start(self):
        try:
            self.redis = await aioredis.create_redis_pool(self.redis_url)
            for name, script in SCRIPTS.items():
                self.scripts[name] = await self.redis.script_load(script)

            self.r_con = await aio_pika.connect_robust(self.rabbit_url)
            self.r_channel = await self.r_con.channel()
            await self.r_channel.set_qos(prefetch_count=1000)
            await self.r_channel.set_qos()

            exchange = await self.r_channel.declare_exchange('events', type='topic')
            queue = await self.r_channel.declare_queue("cache")
            bindings = ("*.guild_create", "*.guild_update", "*.guild_delete", "*.channel_create", "*.channel_update",
                        "*.channel_delete", "*.guild_role_create", "*.guild_role_update", "*.guild_role_delete",
                        "*.guild_member_update", "*.start", "*.disconnect", "*.latency_update")
            for binding in bindings:
                await queue.bind(exchange, routing_key=binding)

            await queue.consume(self._message_received, no_ack=True)

        except ConnectionError:
            traceback.print_exc()
            await asyncio.sleep(5)
            return await self.start()

    def run(self):
        self.loop.create_task(self.start())
        self.loop.run_forever()
