import argparse
import asyncio
import json
from pprint import pprint as print
from typing import Any

from openark import OpenArk


async def request_one_shot(ark: OpenArk, model: str, data: Any) -> None:
    mc = await ark.get_model_channel(model)
    response = await mc.request(value=data, payloads={'value': data})
    print(response)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='OpenARK',
        description='OpenARK Python',
    )
    parser.add_argument(
        'model',
        type=str,
        help='model name',
    )
    parser.add_argument(
        'data',
        type=str,
        help='json data',
    )

    args = parser.parse_args()
    ark = OpenArk()

    # parse data
    data = json.loads(args.data)

    asyncio.run(request_one_shot(ark, args.model, data))
