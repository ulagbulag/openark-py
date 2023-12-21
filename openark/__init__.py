import os

from dotenv import load_dotenv
import kubernetes as kube
import nats

from openark.model import OpenArkGlobalModels, OpenArkModel


__all__ = [
    'OpenArk',
    'OpenArkModel',
    'OpenArkStream',
]


class OpenArk:
    def __init__(self) -> None:
        # take environment variables from .env.
        load_dotenv()

        # load Kubernetes configs
        kube.config.load_kube_config()

        self._namespace = 'dash' or _get_current_namespace()
        self._nc: nats.NATS | None = None
        self._storage_options = {
            'AWS_ACCESS_KEY_ID': os.environ['AWS_ACCESS_KEY_ID'],
            'AWS_ENDPOINT_URL': os.environ['AWS_ENDPOINT_URL'],
            'AWS_REGION': os.environ['AWS_REGION'],
            'AWS_SECRET_ACCESS_KEY': os.environ['AWS_SECRET_ACCESS_KEY'],
        }

    def get_model(self, name: str) -> OpenArkModel:
        return OpenArkModel(
            name=name,
            storage_options=self._storage_options,
        )

    def get_global_models(self) -> OpenArkGlobalModels:
        return OpenArkGlobalModels(
            namespace=self._namespace,
        )


def _get_current_namespace() -> str:
    ns_path = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
    if os.path.exists(ns_path):
        with open(ns_path) as f:
            return f.read().strip()

    try:
        _, active_context = kube.config.list_kube_config_contexts()
        return active_context['context']['namespace']
    except KeyError:
        return 'default'
