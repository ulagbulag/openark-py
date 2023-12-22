import argparse
import asyncio
from pprint import pprint as print

from openark import OpenArk


async def print_data(ark):
    mc = await ark.get_model_channel(args.model)
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

    asyncio.run(print_data(ark))
