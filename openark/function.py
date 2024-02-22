from typing import Any, Coroutine, Dict

from openark.model import OpenArkModel, OpenArkModelChannel, Payload
from openark.messenger import Messenger


class OpenArkFunction:
    def __init__(
        self, /,
        data: dict[str, Any],
        encoder: str,
        messenger: Messenger,
        queued: bool,
        timeout: int,
        storage_options: Dict[str, str] | None = None,
        timestamp: str | None = None,
        user_name: str | None = None,
    ) -> None:
        self._timeout = timeout

        self._input = OpenArkModelChannel(
            encoder=encoder,
            messenger=messenger,
            model=OpenArkModel(
                name=data['spec']['input'],
                storage_options=storage_options,
                timestamp=timestamp,
                user_name=user_name,
            ),
            queued=queued,
        )
        self._output = OpenArkModelChannel(
            encoder=encoder,
            messenger=messenger,
            model=OpenArkModel(
                name=data['spec']['output'],
                storage_options=storage_options,
                timestamp=timestamp,
                user_name=user_name,
            ),
            queued=queued,
        )

    def __call__(
        self, /,
        value: Any = {},
        payloads: dict[str, Payload] = {},
        load_payloads: bool = True,
    ) -> Coroutine[Any, Any, Any]:
        return self._input(
            value=value,
            payloads=payloads,
            load_payloads=load_payloads,
        )

    def get_payload(self, payload: dict[str, Any]) -> Coroutine[Any, Any, Any]:
        return self._input.get_payload(payload)

    def get_payload_url(self, payload: dict[str, Any]) -> str:
        return self._input.get_payload_url(payload)
