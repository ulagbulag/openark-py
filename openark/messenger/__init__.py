import abc
from typing import Optional


class Messenger(metaclass=abc.ABCMeta):
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    async def publisher(
        self,
        topic: str,
        reply: str | None,
    ) -> Optional['Publisher']: ...

    @abc.abstractmethod
    async def service(
        self,
        topic: str,
        timeout_sec: float | None,
    ) -> Optional['Service']: ...

    @abc.abstractmethod
    async def subscriber(
        self,
        topic: str,
        queue: str | None,
    ) -> Optional['Subscriber']: ...


class Publisher(metaclass=abc.ABCMeta):
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    async def __call__(self, data: str) -> None: ...


class Service(metaclass=abc.ABCMeta):
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    async def __call__(self, data: str) -> str: ...


class Subscriber(metaclass=abc.ABCMeta):
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    async def __anext__(self) -> str: ...
