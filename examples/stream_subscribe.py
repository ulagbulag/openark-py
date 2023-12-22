import argparse
import asyncio
from pprint import pprint as print

from openark import OpenArk


async def loop_subscribe(ark: OpenArk, model: str) -> None:
    mc = await ark.get_model_channel(model)
    async for data in mc:
        print(data)


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

    args = parser.parse_args()
    ark = OpenArk()

    asyncio.run(loop_subscribe(ark, args.model))
