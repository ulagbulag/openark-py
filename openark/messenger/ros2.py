import asyncio
from queue import Empty, Queue
import threading
import time

import rclpy
from rclpy.node import Node as RosNode
from std_msgs.msg import String as StringMessage

from openark.messenger import Messenger, Publisher, Service, Subscriber

_DEFAULT_SPIN_INTERVAL = 0.001  # in seconds


class Node(RosNode):
    def __init__(self, context: rclpy.context.Context) -> None:
        super().__init__(
            context=context,
            node_name='FIXME',  # FIXME: speficy node name
        )


class Ros2Messenger(Messenger):
    def __init__(self) -> None:
        super().__init__()

        # init context
        context = rclpy.get_default_context()
        if not context.ok():
            rclpy.init(context=context)

        # init node
        self._node = Node(
            context=context,
        )

        # spin node
        self._handle = asyncio.get_event_loop().create_task(_spin(self._node))

    def __del__(self) -> None:
        if hasattr(self, '_handle'):
            self._handle.cancel()

    def publisher(
        self,
        topic: str,
        reply: str | None,
    ) -> 'Ros2Publisher':
        return Ros2Publisher(
            node=self._node,
            topic=topic,
            reply=reply,
        )

    def service(
        self,
        topic: str,
        timeout_sec: float | None = 10.0,
    ) -> 'Ros2Service':
        return Ros2Service(
            node=self._node,
            topic=topic,
            timeout_sec=timeout_sec,
        )

    def subscriber(
        self,
        topic: str,
        queue: str | None,
    ) -> 'Ros2Subscriber':
        return Ros2Subscriber(
            node=self._node,
            topic=topic,
            queue=queue,
        )


class Ros2Publisher(Publisher):
    def __init__(
        self,
        node: Node,
        topic: str,
        reply: str | None,
    ) -> None:
        super().__init__()
        self._node = node
        self._topic = topic
        self._reply = reply or ''

        self._inner = None

    async def __call__(self, data: str) -> None:
        if self._inner is None:
            self._inner = self._node.create_publisher(
                topic=_parse_topic_name(self._topic),
                msg_type=StringMessage,
                qos_profile=_default_qos_profile(),
            )

        msg = StringMessage()
        msg.data = data
        return self._inner.publish(
            msg=msg,
        )


class Ros2Service(Service):
    def __init__(
        self,
        node: Node,
        topic: str,
        timeout_sec: float | None,
    ) -> None:
        super().__init__()
        self._node = node
        self._topic = topic
        self._timeout_sec = timeout_sec or 10.0

    async def __call__(self, data: bytes) -> str:
        return await self._node.request(
            subject=self._topic,
            payload=data,
            timeout=self._timeout_sec,
        )


class Ros2Subscriber(Subscriber):
    def __init__(
        self,
        node: Node,
        topic: str,
        queue: str | None,
    ) -> None:
        super().__init__()
        self._node = node
        self._topic = topic
        self._queue = queue or ''

        self._data_queue = Queue(maxsize=64)
        self._inner = None

    async def __anext__(self) -> str:
        if self._inner is None:
            def callback(msg) -> None:
                queue = self._data_queue
                with queue.mutex:
                    if 0 < queue.maxsize <= queue._qsize():
                        queue._get()
                    queue._put(msg.data)

            self._inner = self._node.create_subscription(
                topic=_parse_topic_name(self._topic),
                msg_type=StringMessage,
                qos_profile=_default_qos_profile(),
                callback=callback,
            )

        while True:
            try:
                return self._data_queue.get(timeout=_DEFAULT_SPIN_INTERVAL)
            except Empty:
                continue


async def _spin(node: Node):
    """
    Reference: https://robotics.stackexchange.com/a/24305
    """

    cancel = node.create_guard_condition(lambda: None)

    def _spin(
        node: Node,
        future: asyncio.Future,
        event_loop: asyncio.AbstractEventLoop,
    ):
        while not future.cancelled():
            rclpy.spin_once(node)
            time.sleep(_DEFAULT_SPIN_INTERVAL)
        if not future.cancelled():
            event_loop.call_soon_threadsafe(future.set_result, None)

    event_loop = asyncio.get_event_loop()
    spin_task = event_loop.create_future()
    spin_thread = threading.Thread(
        target=_spin,
        args=(node, spin_task, event_loop),
    )
    spin_thread.start()

    try:
        await spin_task
    except asyncio.CancelledError:
        cancel.trigger()
    spin_thread.join()
    node.destroy_guard_condition(cancel)


def _default_qos_profile() -> int:
    return 10


def _parse_topic_name(topic: str) -> str:
    return f'/{topic.replace(".", "/").replace("-", "_")}'
