from typing import Any, Coroutine, Dict

import nats

from openark.model import OpenArkModel, OpenArkModelChannel, Payload


class OpenArkFunction:
    def __init__(
        self,
        data: dict[str, Any],
        nc: nats.NATS,
        queued: bool,
        timeout: int,
        storage_options: Dict[str, str] | None = None,
        timestamp: str | None = None,
        user_name: str | None = None,
    ) -> None:
        self._timeout = timeout

        self._input = OpenArkModelChannel(
            model=OpenArkModel(
                name=data['spec']['input'],
                storage_options=storage_options,
                timestamp=timestamp,
                user_name=user_name,
            ),
            nc=nc,
            queued=queued,
        )
        self._output = OpenArkModelChannel(
            model=OpenArkModel(
                name=data['spec']['output'],
                storage_options=storage_options,
                timestamp=timestamp,
                user_name=user_name,
            ),
            nc=nc,
            queued=queued,
        )

    def __call__(
        self, /,
        payloads: dict[str, Payload] = {},
        **value: dict[str, Any],
    ) -> Coroutine[Any, Any, Any]:
        return self._input.request(
            value=value,
            payloads=payloads,
        )
