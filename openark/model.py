import aiohttp
import asyncio
import base64
import datetime
import io
import json
import logging
from typing import Any, Dict, Optional, TypeVar
from urllib.parse import urlparse

import deltalake as dt
import inflection
import kubernetes as kube
import miniopy_async as minio
import polars as pl

from openark import drawer
from openark.messenger import Messenger

Payload = bytes | dict[str, Any]
T = TypeVar('T')


class OpenArkGlobalNamespace:
    def __init__(self, namespace: str) -> None:
        self._ctx = pl.SQLContext()
        self._namespace = namespace
        self._kube = kube.client.CustomObjectsApi()

        # load models
        for (model, storage_name) in _load_models(self._kube, self._namespace):
            if model._table_name in self._ctx.tables():
                continue

            logging.info(f'Loading model: {model._name}')
            try:
                df = model.to_polars()
            except dt.exceptions.TableNotFoundError:
                df = None
            if df is None or df.is_empty():
                logging.warn(
                    f'Model {model._name} is not inited yet on {storage_name}; skipping...'
                )
                continue
            self._ctx.register(
                name=model._table_name,
                frame=df,
            )

    def sql(self, query: str) -> pl.LazyFrame:
        return self._ctx.execute(query)

    def sql_and_draw(self, query: str, style: Optional[str] = None) -> None:
        # collect data frame
        lf = self.sql(query)

        # draw
        drawer.draw(lf, style)


class OpenArkModel:
    def __init__(
        self,
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
        self._table_name = inflection.underscore(name)
        self._table_uri = f's3a://{name}/metadata/'
        self._storage_options = storage_options
        self._timestamp = (timestamp or get_timestamp()).replace(':', '-')
        self._user_name = user_name or 'openark-py'
        self._version = version

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

    def _load_minio_client(self) -> minio.Minio:
        endpoint = urlparse(self._storage_options['AWS_ENDPOINT_URL'])

        if self._minio is None:
            self._minio = minio.Minio(
                endpoint=endpoint.hostname,
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
    ) -> str:
        if not isinstance(value, dict):
            value = {
                'value': value,
            }

        payloads_dumped = await asyncio.gather(*(
            self._put(key, value)
            for key, value in payloads.items()
        ))
        payloads_map = dict(zip(payloads, payloads_dumped))

        return json.dumps({
            '__timestamp': get_timestamp(),
            '__payloads': payloads_dumped,
            **_replace_payloads(value, payloads_map),
        })

    async def _get(
        self,
        key: str,
        session: aiohttp.ClientSession | None = None,
    ) -> bytes:
        client = self._load_minio_client()

        session_is_entered = session is None
        if session_is_entered:
            session = await aiohttp.ClientSession().__aenter__()

        response = await client.get_object(
            bucket_name=self._name,
            object_name=key,
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
            'key': response.object_name,
            'model': self._name,
            'storage': 'S3',
        }

    def to_delta(self) -> dt.DeltaTable:
        return dt.DeltaTable(
            table_uri=self._table_uri,
            storage_options=self._storage_options,
            version=self._version,
        )

    def to_polars(self) -> pl.DataFrame:
        return pl.read_delta(
            source=self._table_uri,
            storage_options=self._storage_options,
        )


class OpenArkModelChannel:
    def __init__(
        self,
        messenger: Messenger,
        model: OpenArkModel,
        queued: bool,
    ) -> None:
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

    async def __anext__(self) -> Any:
        if self._subscriber is None:
            raise Exception(
                f'Subscribing is not supported on this messenger type'
            )

        while True:
            msg = await self._subscriber.__anext__()
            try:
                decoded = json.loads(msg)
            except json.decoder.JSONDecodeError:
                continue
            return decoded

    async def __call__(
        self,
        value: Any = {},
        payloads: dict[str, Payload] = {},
    ) -> None:
        if self._service is None:
            raise Exception(f'Service is not supported on this messenger type')

        return await self._service(
            data=await self._model._build_message(
                value=value,
                payloads=payloads,
            ),
        )

    def __enter__(self) -> 'OpenArkModelChannel':
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        pass

    async def publish(
        self,
        value: Any = {},
        payloads: dict[str, Payload] = {},
    ) -> None:
        if self._publisher is None:
            raise Exception(
                f'Publishing is not supported on this messenger type'
            )

        return await self._publisher(
            data=await self._model._build_message(
                value=value,
                payloads=payloads,
            ),
        )

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


def _replace_payloads(data: T, payloads: dict[str, dict[str, Any]]) -> T:
    if isinstance(data, tuple) or isinstance(data, list):
        return [
            _replace_payloads(item, payloads)
            for item in data
        ]
    elif isinstance(data, dict):
        return {
            key: _replace_payloads(value, payloads)
            for key, value in data.items()
        }
    elif isinstance(data, str):
        scheme = '@data:'
        if isinstance(data, str) and data.startswith(scheme):
            type_, *key = data[len(scheme):].split(',')
            key = ','.join(key)
            return f'{scheme}{type_},{payloads[key]["key"]}'
        else:
            return data
    else:
        return data


def get_timestamp() -> str:
    return f'{datetime.datetime.utcnow().isoformat()}Z'
