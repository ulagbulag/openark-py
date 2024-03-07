import aiohttp
import asyncio
import base64
import datetime
import io
import json
import logging
import os
from typing import Any, Coroutine, Dict, Optional, TypeVar
from urllib.parse import urlparse

import deltalake as dl
import inflection
import kubernetes as kube
import lancedb
from lancedb.table import LanceTable
import miniopy_async as minio
import polars as pl

from openark import codec, drawer
from openark.messenger import Messenger

Payload = bytes | dict[str, Any]
T = TypeVar('T')


class OpenArkGlobalNamespace:
    def __init__(self, namespace: str) -> None:
        self._delta_ctx_instance: pl.SQLContext = None
        self._namespace = namespace
        self._kube = kube.client.CustomObjectsApi()

        # load models
        self._models: dict[str, OpenArkModel] = {}
        for (model, storage_name) in _load_models(self._kube, self._namespace):
            if model._table_name in self._models:
                continue

            try:
                check_init = model.to_delta().version() > 0
            except dl.exceptions.TableNotFoundError:
                check_init = False
            if not check_init:
                logging.warn(
                    f'Model {model._name} is not inited yet on {storage_name}; skipping...'
                )
                continue

            logging.info(f'Loading model: {model._name}')
            self._models[model._table_name] = model

    def _build_delta_ctx(self) -> pl.SQLContext:
        ctx = pl.SQLContext()
        for model in self._models.values():
            ctx.register(
                name=model._table_name,
                frame=model.to_delta_polars(),
            )
        return ctx

    def delta_ctx(self, /, refresh: bool = False) -> pl.SQLContext:
        if self._delta_ctx_instance is None or refresh:
            self._delta_ctx_instance = self._build_delta_ctx()
        return self._delta_ctx_instance

    def delta_sql(self, query: str, *, refresh: bool = False) -> pl.LazyFrame:
        return self.delta_ctx(refresh=refresh).execute(query)

    def delta_sql_and_draw(
        self,
        query: str,
        style: Optional[str] = None,
        *,
        refresh: bool = False,
    ) -> None:
        # collect data frame
        lf = self.delta_sql(query, refresh=refresh)

        # draw
        drawer.draw(lf, style)

    def update(self) -> None:
        self._delta_ctx_instance = self._build_delta_ctx()


class OpenArkModel:
    def __init__(
        self, /,
        name: str,
        version: int | None = None,
        storage_options: Dict[str, str] | None = None,
        timestamp: str | None = None,
        user_name: str | None = None,
    ) -> None:
        if 'AWS_ENDPOINT_URL' in storage_options:
            endpoint_url = storage_options['AWS_ENDPOINT_URL']
            allow_http = 'true' \
                if endpoint_url.startswith('http://') \
                else 'false'
            storage_options.setdefault('AWS_ALLOW_HTTP', allow_http)
            storage_options.setdefault('AWS_S3_ALLOW_UNSAFE_RENAME', 'true')

        self._name = name
        self._table_name = inflection.underscore(name.replace('.', '_'))
        self._table_uri = f's3a://{name}/metadata/'
        self._storage_options = storage_options
        self._timestamp = (timestamp or get_timestamp()).replace(':', '-')
        self._user_name = user_name or 'openark-py'
        self._version = version

        self._endpoint = urlparse(self._storage_options['AWS_ENDPOINT_URL'])
        self._minio: minio.Minio | None = None

    @classmethod
    def load_object_storage(
        cls,
        namespace: str,
        model_name: str,
        data: dict[str, Any],
    ) -> 'OpenArkModel':
        # parse account secret ref
        if 'borrowed' in data:
            endpoint = data['endpoint']
            secret_ref = data['secretRef']
        else:
            endpoint = f'http://minio.{namespace}.svc'
            secret_ref = {
                'mapAccessKey': 'CONSOLE_ACCESS_KEY',
                'mapSecretKey': 'CONSOLE_SECRET_KEY',
                'name': 'object-storage-user-0',
            }

        # load account secret
        api = kube.client.CoreV1Api()
        secret = api.read_namespaced_secret(
            name=secret_ref['name'],
            namespace=namespace,
        )

        def decode_secret(key: str) -> str:
            return base64.standard_b64decode(
                secret.data[secret_ref[key]].encode('ascii'),
            ).decode('ascii')

        storage_options = {
            'AWS_ACCESS_KEY_ID': decode_secret('mapAccessKey'),
            'AWS_ENDPOINT_URL': endpoint,
            'AWS_REGION': 'us-east-1',
            'AWS_SECRET_ACCESS_KEY': decode_secret('mapSecretKey'),
        }

        # create model
        return OpenArkModel(
            name=model_name,
            storage_options=storage_options,
        )

    async def get_payload(self, payload: dict[str, Any]) -> bytes:
        async with aiohttp.ClientSession() as session:
            return await self._get(
                session=session,
                payload=payload,
            )

    def get_payload_url(self, payload: dict[str, Any]) -> str:
        return f'{self._endpoint.geturl()}/{payload["model"]}/{payload["path"]}'

    def _load_minio_client(self) -> minio.Minio:
        if self._minio is None:
            self._minio = minio.Minio(
                endpoint=self._endpoint.hostname,
                access_key=self._storage_options['AWS_ACCESS_KEY_ID'],
                secret_key=self._storage_options['AWS_SECRET_ACCESS_KEY'],
                secure=not self._storage_options.get('AWS_ALLOW_HTTP', False),
                region=self._storage_options['AWS_REGION'],
            )
        return self._minio

    async def _build_message(
        self,
        value: Any = {},
        payloads: dict[str, Payload] = {},
    ) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {
                'value': value,
            }

        payloads_dumped = await asyncio.gather(*(
            self._put(key, value)
            for key, value in payloads.items()
        ))

        return {
            '__timestamp': get_timestamp(),
            '__payloads': payloads_dumped,
            **value,
        }

    async def _get(
        self,
        payload: dict[str, Any],
        session: aiohttp.ClientSession | None = None,
    ) -> bytes:
        storage_type = payload['storage']
        match storage_type:
            case 'Passthrough' | None:
                return payload['value']
            case 'S3':
                return await self._get_minio(
                    payload=payload,
                    session=session,
                )
            case _:
                raise ValueError(
                    f'Unsupported OpenARK storage type: {storage_type}')

    async def _get_minio(
        self,
        payload: dict[str, Any],
        session: aiohttp.ClientSession | None = None,
    ) -> Optional[bytes]:
        model = payload['model']
        path = payload['path']

        # check data
        if model is None or path is None:
            return payload

        client = self._load_minio_client()

        session_is_entered = session is None
        if session_is_entered:
            session = await aiohttp.ClientSession().__aenter__()

        response = await client.get_object(
            bucket_name=payload['model'],
            object_name=payload['path'],
            session=session,
        )
        content = await response.content.read()

        if session_is_entered:
            await session.__aexit__()
        return content

    async def _put(
        self,
        key: str,
        value: Payload,
    ) -> dict[str, Any]:
        client = self._load_minio_client()

        if isinstance(value, bytes):
            data = value
        else:
            data = json.dumps(value).encode('utf-8')

        raw_key = f'payloads/{self._user_name}/{self._timestamp}/{key}'
        response = await client.put_object(
            bucket_name=self._name,
            object_name=raw_key,
            data=io.BytesIO(data),
            length=len(data),
        )

        return {
            'key': key,
            'model': self._name,
            'path': response.object_name,
            'storage': 'S3',
        }

    def to_delta(self) -> dl.DeltaTable:
        return dl.DeltaTable(
            table_uri=self._table_uri,
            storage_options=self._storage_options,
            version=self._version,
        )

    def to_delta_polars(self) -> pl.LazyFrame:
        return pl.scan_delta(
            source=self._table_uri,
            storage_options=self._storage_options,
        )

    def to_lancedb(
        self,
        read_consistency_interval: Optional[datetime.timedelta] = datetime.timedelta(
            seconds=5),
    ) -> LanceTable:
        # currently (lancedb==0.6.2), it uses env to configure
        os.environ['AWS_ALLOW_HTTP'] = 'true' \
            if self._storage_options.get('AWS_ALLOW_HTTP', False) \
            else 'false'
        os.environ.setdefault(
            'AWS_ENDPOINT',
            self._storage_options['AWS_ENDPOINT_URL'],
        )

        conn = lancedb.connect(
            uri=f's3{self._table_uri[3:]}',
            read_consistency_interval=read_consistency_interval,
        )
        return conn.open_table('metadata')


class OpenArkModelChannel:
    def __init__(
        self, /,
        encoder: str,
        messenger: Messenger,
        model: OpenArkModel,
        queued: bool,
    ) -> None:
        self._encoder = encoder
        self._messenger = messenger
        self._model = model
        self._queued = queued
        self._reply: str | None = None
        self._service_timeout_sec: float | None = 10.0

        self._publisher = self._messenger.publisher(
            topic=self.name,
            reply=self._reply,
        )
        self._service = self._messenger.service(
            topic=self.name,
            timeout_sec=self._service_timeout_sec,
        )
        self._subscriber = self._messenger.subscriber(
            topic=self.name,
            queue=self.name if self._queued else None,
        )

    def __aiter__(self) -> 'OpenArkModelChannel':
        return self

    async def __anext__(self) -> dict[str, Any]:
        if self._subscriber is None:
            raise Exception(
                f'Subscribing is not supported on this messenger type'
            )

        while True:
            data = await self._subscriber.__anext__()
            message = codec.loads(data)
            if message is None:
                continue
            return await self._load_payloads(message)

    async def __call__(
        self,
        value: Any = {},
        payloads: dict[str, Payload] = {},
        load_payloads: bool = True,
    ) -> None:
        if self._service is None:
            raise Exception(f'Service is not supported on this messenger type')

        message = await self._model._build_message(
            value=value,
            payloads=payloads,
        )
        data = await self._service(
            data=codec.dumps(message, codec=self._encoder),
        )
        message = codec.loads(data)

        if load_payloads:
            return await self._load_payloads(message)
        else:
            return message

    async def _load_payloads(self, message: dict[str, Any]) -> dict[str, Any]:
        if message['__payloads']:
            async with aiohttp.ClientSession() as session:
                async def load_payload(payload): return {
                    **payload,
                    'value': await self._model._get(
                        session=session,
                        payload=payload,
                    ),
                }
                message['__payloads'] = await asyncio.gather(*(
                    load_payload(payload)
                    for payload in message['__payloads']
                ))
        return message

    def __enter__(self) -> 'OpenArkModelChannel':
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        pass

    def get_payload(self, payload: dict[str, Any]) -> Coroutine[Any, Any, bytes]:
        return self._model.get_payload(payload)

    def get_payload_url(self, payload: dict[str, Any]) -> str:
        return self._model.get_payload_url(payload)

    async def publish(
        self,
        value: Any = {},
        payloads: dict[str, Payload] = {},
    ) -> dict[str, Any]:
        if self._publisher is None:
            raise Exception(
                f'Publishing is not supported on this messenger type'
            )

        message = await self._model._build_message(
            value=value,
            payloads=payloads,
        )
        await self._publisher(
            data=codec.dumps(message, codec=self._encoder),
        )
        return message

    @property
    def name(self) -> str:
        return self._model._name


def _load_models(kube: kube.client.CustomObjectsApi, namespace: str) -> list[tuple[OpenArkModel, str]]:
    bindings = kube.list_namespaced_custom_object(
        group='dash.ulagbulag.io',
        plural='modelstoragebindings',
        version='v1alpha1',
        namespace=namespace,
    )['items']

    models = []
    unique_keys = set()
    for binding in bindings:
        model_name: str = binding['spec']['model']
        storage_name: str = _get_storage_target(binding['spec']['storage'])

        # unique by key
        unique_key = (model_name, storage_name)
        if unique_key in unique_keys:
            continue
        unique_keys.add(unique_key)

        # test status
        status = binding['status']
        if status.get('state', 'Pending') != 'Ready':
            continue

        # test storage kind
        storage = status['storage']
        object_storage = _get_storage_target(
            storage).get('objectStorage', None)
        if object_storage is None:
            logging.warn(
                f'Sorry, but the non-object storage is not supported yet: {model_name}'
            )
            continue

        model = OpenArkModel.load_object_storage(
            namespace=namespace,
            model_name=model_name,
            data=object_storage,
        )
        models.append((model, storage_name))
    return models


def _get_storage_target(storage: dict[str, Any]) -> Any:
    child = storage['cloned'] if 'cloned' in storage else storage['owned']
    return child['target']


def get_timestamp() -> str:
    return f'{datetime.datetime.utcnow().isoformat()}Z'
