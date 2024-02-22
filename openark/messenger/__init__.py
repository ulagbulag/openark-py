import abc
from typing import Optional


class Messenger(metaclass=abc.ABCMeta):
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    async def close(self) -> None: ...

    @abc.abstractmethod
    def publisher(
        self,
        topic: str,
        reply: str | None,
    ) -> Optional['Publisher']: ...

    @abc.abstractmethod
    def service(
        self,
        topic: str,
        timeout_sec: float | None,
    ) -> Optional['Service']: ...

    @abc.abstractmethod
    def subscriber(
        self,
        topic: str,
        queue: str | None,
    ) -> Optional['Subscriber']: ...


class Publisher(metaclass=abc.ABCMeta):
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    async def __call__(self, data: bytes | bytearray) -> None: ...


class Service(metaclass=abc.ABCMeta):
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    async def __call__(self, data: bytes | bytearray) -> bytes: ...


class Subscriber(metaclass=abc.ABCMeta):
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    async def __anext__(self) -> bytes: ...
