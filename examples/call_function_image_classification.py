import argparse
import asyncio
import os
from pprint import pprint as print

from openark import OpenArk


async def call_function(
    ark: OpenArk,
    filename: str,
) -> None:
    # define a function
    function_name = 'image-classification'
    function = await ark.get_function(function_name)

    # check file
    if not os.path.exists(filename):
        raise ValueError(f"Image file {filename:?} not found!")

    # load payload files
    payload_image_key = 'my-image-data.bin'
    payloads = {
        f'{payload_image_key}': open(filename, 'rb').read(),
    }

    # make an input value
    input = {
        "images": [f"@data:image,{payload_image_key}"],
    }

    # call the function and get a response
    output = await function(
        input,
        payloads=payloads,
    )

    # print the output values
    print(output)


if __name__ == '__main__':
    # define command-line parameters
    parser = argparse.ArgumentParser(
        prog='OpenARK',
        description='OpenARK Python',
    )
    parser.add_argument(
        'filename',
        type=str,
        help='an image file',
    )

    # parse command-line parameters
    args = parser.parse_args()
    ark = OpenArk()

    asyncio.run(call_function(ark, args.filename))
