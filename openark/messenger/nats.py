import os

import nats

from openark.messenger import Messenger, Publisher, Service, Subscriber


class NatsMessenger(Messenger):
    def __init__(self, nc: nats.NATS) -> None:
        super().__init__()
        self._nc = nc

    async def close(self) -> None:
        return await self._nc.close()

    def publisher(
        self,
        topic: str,
        reply: str | None,
    ) -> 'NatsPublisher':
        return NatsPublisher(
            nc=self._nc,
            topic=topic,
            reply=reply,
        )

    def service(
        self,
        topic: str,
        timeout_sec: float | None = 10.0,
    ) -> 'NatsService':
        return NatsService(
            nc=self._nc,
            topic=topic,
            timeout_sec=timeout_sec,
        )

    def subscriber(
        self,
        topic: str,
        queue: str | None,
    ) -> 'NatsSubscriber':
        return NatsSubscriber(
            nc=self._nc,
            topic=topic,
            queue=queue,
        )


class NatsPublisher(Publisher):
    def __init__(
        self,
        nc: nats.NATS,
        topic: str,
        reply: str | None,
    ) -> None:
        super().__init__()
        self._nc = nc
        self._topic = topic
        self._reply = reply or ''

    async def __call__(self, data: bytes | bytearray) -> None:
        return await self._nc.publish(
            subject=self._topic,
            payload=data,
            reply=self._reply,
        )


class NatsService(Service):
    def __init__(
        self,
        nc: nats.NATS,
        topic: str,
        timeout_sec: float | None,
    ) -> None:
        super().__init__()
        self._nc = nc
        self._topic = topic
        self._timeout_sec = timeout_sec or 10.0

    async def __call__(self, data: bytes | bytearray) -> bytes:
        msg = await self._nc.request(
            subject=self._topic,
            payload=data,
            timeout=self._timeout_sec,
        )
        return msg.data


class NatsSubscriber(Subscriber):
    def __init__(
        self,
        nc: nats.NATS,
        topic: str,
        queue: str | None,
    ) -> None:
        super().__init__()
        self._nc = nc
        self._topic = topic
        self._queue = queue or ''

        self._inner = None

    async def __anext__(self) -> bytes:
        if self._inner is None:
            self._inner = await self._nc.subscribe(
                subject=self._topic,
                queue=self._queue,
                pending_msgs_limit=1
                if is_drop_allowed()
                else nats.aio.subscription.DEFAULT_SUB_PENDING_MSGS_LIMIT,
            )

        msg = await self._inner.next_msg(timeout=None)
        return msg.data


def is_drop_allowed() -> bool:
    return os.environ.get('NATS_ALLOW_DROP', 'false').lower() == 'true'
