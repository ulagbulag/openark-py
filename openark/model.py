import base64
import logging
from typing import Any, Dict, Optional

import deltalake as dt
import inflection
import kubernetes as kube
import polars as pl

from openark import drawer


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
            df = model.to_polars()
            if df.is_empty():
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
        self._version = version

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


def _load_models(kube: kube.client.CustomObjectsApi, namespace: str) -> list[tuple[OpenArkModel, str]]:
    bindings = kube.list_namespaced_custom_object(
        group='dash.ulagbulag.io',
        version='v1alpha1',
        namespace=namespace,
        plural='modelstoragebindings',
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
