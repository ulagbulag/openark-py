import argparse
import asyncio
import json
import os
from pprint import pprint as print
from typing import Any

from openark import OpenArk


async def call_function(
    ark: OpenArk,
    function_name: str,
    value: dict[str, Any],
    files: list[str],
) -> None:
    # load payload files
    payloads = {
        os.path.basename(filename): open(filename, 'rb').read()
        for filename in files
    }

    function = await ark.get_function(function_name)
    output = await function(
        **value,
        payloads=payloads,
    )
    print(output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='OpenARK',
        description='OpenARK Python',
    )
    parser.add_argument(
        'function',
        type=str,
        help='a function name to be executed',
    )
    parser.add_argument(
        'value',
        type=str,
        help='a JSON-formatted function input',
    )
    parser.add_argument(
        'files',
        nargs=argparse.REMAINDER,
        help='function payload files',
    )

    args = parser.parse_args()
    ark = OpenArk()

    # parse data
    value = json.loads(args.value)
    files = args.files

    asyncio.run(call_function(ark, args.function, value, files))
