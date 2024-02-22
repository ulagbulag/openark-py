import argparse
import asyncio
import os

from openark import OpenArk


async def upload_image(
    ark: OpenArk,
    filename: str,
) -> None:
    # define a model
    model_name = 'image'
    model = await ark.get_model_channel(model_name)

    # check file
    if not os.path.exists(filename):
        raise ValueError(f"Image file {filename!r} not found!")

    # load payload files
    filename_ext = filename.split('.')[-1]
    payload_image_key = f'my-image-data.{filename_ext}'
    payloads = {
        f'{payload_image_key}': open(filename, 'rb').read(),
    }

    # make an input value
    input = {
        "images": [f"@data:image,{payload_image_key}"],
    }

    # send the image
    message = await model.publish(
        input,
        payloads=payloads,
    )

    # load image
    image_payload = message['__payloads'][0]
    image = await model.get_payload(image_payload)
    image_url = model.get_payload_url(image_payload)

    # print the output image's size and URL
    print(f'image size: {len(image)}')
    print(f'image url: {image_url}')


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

    asyncio.run(upload_image(ark, args.filename))
