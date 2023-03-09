from airflow.utils.eventbus import BaseEventBusBackend
import redis.asyncio as redis
from os import getpid

pid = getpid()


class EventBus(BaseEventBusBackend):
    def __init__(self):
        self.redis = None
        self.pubsub = None

    async def connect(self):
        self.redis = await redis.from_url("redis://localhost")
        self.pubsub = self.redis.pubsub()

    async def send(self, channel, value):
        await self.redis.publish(channel, value)

    async def subscribe(self, channel):
        self.log.debug(f"PID {pid} Subscribing to {channel}")
        return await self.pubsub.subscribe(channel)

    async def get_message(self, channel):
        self.log.debug(f"PID {pid} Awaiting message on {channel}")
        while True:
            result = await self.pubsub.get_message(ignore_subscribe_messages=True, timeout=3)
            if result is not None:
                self.log.debug(f"Got a message on {channel}")
                return result
            self.log.debug(f"Got nothing after timeout")


class BaseStateBackend:
    pass
