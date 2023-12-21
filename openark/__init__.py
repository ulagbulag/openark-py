import os

from dotenv import load_dotenv
import nats

from openark.model import OpenArkModel


__all__ = [
    'OpenArk',
    'OpenArkModel',
    'OpenArkStream',
]


class OpenArk:
    def __init__(self) -> None:
        # take environment variables from .env.
        load_dotenv()

        self._nc: nats.NATS | None = None

    def get_model(self, name: str) -> OpenArkModel:
        return OpenArkModel(
            model_name=name,
            table_uri=f's3a://{name}/metadata/',
            storage_options={
                'AWS_ACCESS_KEY_ID': os.environ['AWS_ACCESS_KEY_ID'],
                'AWS_ENDPOINT_URL': os.environ['AWS_ENDPOINT_URL'],
                'AWS_REGION': os.environ['AWS_REGION'],
                'AWS_SECRET_ACCESS_KEY': os.environ['AWS_SECRET_ACCESS_KEY'],
            },
        )
