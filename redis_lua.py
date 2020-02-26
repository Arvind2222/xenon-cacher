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
        {PAIR_SCRIPT('role', 'paired')}
        redis.call('hmset', 'roles_' .. role.id, unpack(paired))
        redis.call('sadd', 'guilds_' .. role.guild_id .. '_roles', role.id)
    end
end

data.roles = nil

if data.channels ~= nil then
    for i, channel in pairs(data.channels) do
        channel.guild_id = data.id
        {PAIR_SCRIPT('channel', 'paired')}
        redis.call('hmset', 'channels_' .. channel.id, unpack(paired))
        redis.call('sadd', 'guilds_' .. channel.guild_id .. '_channels', channel.id)
    end
end

data.channels = nil

if data.members ~= nil then
    for i, member in pairs(data.members) do
        member.guild_id = data.id
        {PAIR_SCRIPT('member', 'paired')}
        redis.call('hmset', 'guilds_' .. member.guild_id .. '_members_' .. member.user.id, unpack(paired))
        redis.call('sadd', 'guilds_' .. member.guild_id .. '_members', member.user.id)
    end
end

data.members = nil

{PAIR_SCRIPT('data', 'paired')}
redis.call('hmset', 'guilds_' .. data.id, unpack(paired))
redis.call('sadd', 'guilds', data.id)

return 1
    """,

    guild_delete="""
local data = cmsgpack.unpack(ARGV[1])
redis.call('del', 'guilds_' .. data.id)
redis.call('srem', 'guilds', data.id)

local keys = {}

local channels = redis.call('smembers', 'guilds_' .. data.id .. '_channels')
for i, channel_id in pairs(channels) do
    table.insert(keys, 'channels_' .. channel_id)
end

local roles = redis.call('smembers', 'guilds_' .. data.id .. '_roles')
for i, role_id in pairs(roles) do
    table.insert(keys, 'roles_' .. role_id)
end

local members = redis.call('smembers', 'guilds_' .. data.id .. '_members')
for i, member_id in pairs(members) do
    table.insert(keys, 'guilds_' .. data.id .. '_members_' .. member_id)
end

redis.call('del', unpack(keys))
    """,

    channel_create=f"""
local data = cmsgpack.unpack(ARGV[1])
{PAIR_SCRIPT('data', 'paired')}

redis.call('hmset', 'channels_' .. data.id, unpack(paired))
if data.guild_id ~= nil then
    redis.call('sadd', 'guilds_' .. data.guild_id .. '_channels', data.id)
else
    redis.call('sadd', 'dm_channels', data.id)
end

return 1
    """,

    channel_update=f"""
local data = cmsgpack.unpack(ARGV[1])
{PAIR_SCRIPT('data', 'paired')}

redis.call('hmset', 'channels_' .. data.id, unpack(paired))

return 1
    """,

    channel_delete=f"""
local data = cmsgpack.unpack(ARGV[1])
redis.call('del', 'channels_' .. data.id)

if data.guild_id ~= nil then
    redis.call('srem', 'guilds_' .. data.guild_id .. '_channels', data.id)
else
    redis.call('srem', 'dm_channels', data.id)
end

return 1
    """,

    guild_role_create=f"""
local data = cmsgpack.unpack(ARGV[1])
local role = data.role
role.guild_id = data.guild_id
{PAIR_SCRIPT('role', 'paired')}

redis.call('hmset', 'roles_' .. role.id, unpack(paired))
redis.call('sadd', 'guilds_' .. data.guild_id .. '_roles', role.id)

return 1
""",

    guild_role_update=f"""
local data = cmsgpack.unpack(ARGV[1])
local role = data.role
role.guild_id = data.guild_id
{PAIR_SCRIPT('role', 'paired')}

redis.call('hmset', 'roles_' .. role.id, unpack(paired))

return 1
""",

    guild_role_delete=f"""
local data = cmsgpack.unpack(ARGV[1])
redis.call('del', 'roles_' .. data.role_id)
redis.call('srem', 'guilds_' .. data.guild_id .. '_roles', data.role_id)

return 1
""",

    guild_member_add=f"""
local data = cmsgpack.unpack(ARGV[1])
{PAIR_SCRIPT('data', 'paired')}

redis.call('hmset', 'guilds_' .. data.guild_id .. '_members_' .. data.user.id, unpack(paired))
redis.call('sadd', 'guilds_' .. data.guild_id .. '_members', data.user.id)

return 1
""",

    guild_member_update=f"""
local data = cmsgpack.unpack(ARGV[1])
{PAIR_SCRIPT('data', 'paired')}

redis.call('hmset', 'guilds_' .. data.guild_id .. '_members_' .. data.user.id, unpack(paired))

return 1
""",

    guild_member_remove=f"""
local data = cmsgpack.unpack(ARGV[1])

redis.call('del', 'guilds_' .. data.guild_id .. '_members_' .. data.user.id)
redis.call('srem', 'guilds_' .. data.guild_id .. '_members', data.user.id)

return 1
"""
)

SCRIPTS["guild_update"] = SCRIPTS["guild_create"]


print("\n".join(f"{i + 1} {l}" for i, l in enumerate(SCRIPTS["guild_create"].splitlines())))


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
            queue = await self.r_channel.declare_queue("cache")
            await queue.consume(self._message_received, no_ack=True)

        except ConnectionError:
            traceback.print_exc()
            await asyncio.sleep(5)
            return await self.start()

    def run(self):
        self.loop.create_task(self.start())
        self.loop.run_forever()
