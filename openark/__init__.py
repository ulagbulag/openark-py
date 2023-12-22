import os
from typing import Optional

from dotenv import load_dotenv
from IPython import get_ipython
import kubernetes as kube
import nats

from openark.magic import OpenArkMagic
from openark.model import OpenArkGlobalNamespace, OpenArkModel, OpenArkModelChannel


__all__ = [
    'OpenArk',
    'OpenArkModel',
    'OpenArkStream',
]


class OpenArk:
    _GLOBAL: 'OpenArk' = None

    def __new__(
        cls,
        register_global: bool = True,
        reuse_global_if_registered: bool = True,
    ) -> 'OpenArk':
        if register_global:
            instance = cls.get_global_instance()
            if instance is not None:
                if reuse_global_if_registered:
                    return instance
                raise ValueError(
                    'Only one OpenARK instance can be inited as global'
                )

        return super(OpenArk, cls).__new__(cls)

    def __init__(
        self,
        register_global: bool = True,
        reuse_global_if_registered: bool = True,
    ) -> None:
        # try init
        if getattr(self, '_inited', False):
            return
        self._inited = True

        # take environment variables from .env.
        load_dotenv()

        # load Kubernetes configs
        kube.config.load_kube_config()

        # register global instance
        if register_global:
            type(self)._GLOBAL = self

            # register iPython magics
            ipy = get_ipython()
            if ipy is not None:
                ipy.register_magics(OpenArkMagic)

        self._global_namespace: OpenArkGlobalNamespace | None = None
        self._namespace = 'dash' or _get_current_namespace()
        self._nc: nats.NATS | None = None
        self._storage_options = {
            'AWS_ACCESS_KEY_ID': os.environ['AWS_ACCESS_KEY_ID'],
            'AWS_ENDPOINT_URL': os.environ['AWS_ENDPOINT_URL'],
            'AWS_REGION': os.environ['AWS_REGION'],
            'AWS_SECRET_ACCESS_KEY': os.environ['AWS_SECRET_ACCESS_KEY'],
        }

    @classmethod
    def get_global_instance(cls) -> Optional['OpenArk']:
        return cls._GLOBAL

    def get_model(self, name: str) -> OpenArkModel:
        return OpenArkModel(
            name=name,
            storage_options=self._storage_options,
        )

    async def get_model_channel(self, name: str) -> OpenArkModelChannel:
        return OpenArkModelChannel(
            name=name,
            nc=await self._load_nats_channel(),
            queued=os.environ.get('PIPE_QUEUE_GROUP', 'false') == 'true',
        )

    def get_global_namespace(self) -> OpenArkGlobalNamespace:
        if self._global_namespace is None:
            self._global_namespace = OpenArkGlobalNamespace(
                namespace=self._namespace,
            )
        return self._global_namespace

    async def _load_nats_channel(self):
        if self._nc is None:
            addrs = []
            for addr in os.environ['NATS_ADDRS'].split(','):
                addr = addr.strip()
                if len(addr) == 0:
                    continue

                _PROTOCOL = 'nats://'
                if not addr.startswith(_PROTOCOL):
                    addr = f'{_PROTOCOL}{addr}:4222'
                addrs.append(addr)

            if len(addrs) == 0:
                raise ValueError(
                    'no NATS addrs are given (no "NATS_ADDRS" environment variable)'
                )

            self._nc = await nats.connect(
                servers=addrs,
                user=os.environ['NATS_ACCOUNT'],
                password=_load_nats_token(),
            )
        return self._nc


def _get_current_namespace() -> str:
    ns_path = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
    if os.path.exists(ns_path):
        with open(ns_path) as f:
            return f.read().split('\n')[0].strip()

    try:
        _, active_context = kube.config.list_kube_config_contexts()
        return active_context['context']['namespace']
    except KeyError:
        return 'default'


def _load_nats_token() -> str:
    token_path = os.environ['NATS_PASSWORD_PATH']
    if not os.path.exists(token_path):
        raise Exception(
            f'NATS token path not found: {token_path}'
        )
    with open(token_path) as f:
        return f.read().strip()
