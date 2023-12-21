from pathlib import Path
from typing import Dict

import deltalake as dt
import polars as pl


class OpenArkModel:
    def __init__(
        self,
        model_name: str,
        table_uri: str | Path,
        version: int | None = None,
        storage_options: Dict[str, str] | None = None,
    ):
        if 'AWS_ENDPOINT_URL' in storage_options:
            endpoint_url = storage_options['AWS_ENDPOINT_URL']
            allow_http = 'true' \
                if endpoint_url.startswith('http://') \
                else 'false'
            storage_options.setdefault('AWS_ALLOW_HTTP', allow_http)
            storage_options.setdefault('AWS_S3_ALLOW_UNSAFE_RENAME', 'true')

        self._model_name = model_name
        self._table_uri = table_uri
        self._storage_options = storage_options
        self._version = version

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
