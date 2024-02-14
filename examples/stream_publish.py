import argparse
import asyncio
import itertools
from pprint import pprint as print

from openark import OpenArk


async def loop_publish(ark: OpenArk, model: str) -> None:
    mc = await ark.get_model_channel(model)
    for index in itertools.count(start=0):
        data = {
            'kind': 'stream_publish_example',
            'index': index,
        }
        await mc.publish(data)
        print(f'Sent: {index}')
        await asyncio.sleep(1)  # sleep 1 sec


if __name__ == '__main__':
    # define command-line parameters
    parser = argparse.ArgumentParser(
        prog='OpenARK',
        description='OpenARK Python',
    )
    parser.add_argument(
        'model',
        type=str,
        help='model name',
    )

    # parse command-line parameters
    args = parser.parse_args()
    ark = OpenArk()

    asyncio.run(loop_publish(ark, args.model))
