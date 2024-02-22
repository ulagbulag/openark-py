import argparse
import asyncio
import os

from openark import OpenArk


async def upload_image(
    ark: OpenArk,
    save_dir: str,
) -> None:
    # define a model
    model_name = 'image'
    model = await ark.get_model_channel(model_name)

    # send the image
    message = await model.__anext__()

    # inspect image
    image_payload = message['__payloads'][0]
    image = await model.get_payload(image_payload)
    image_filename = f'{save_dir}/{os.path.basename(image_payload["key"])}'
    image_url = model.get_payload_url(image_payload)

    # print the output image's size and URL
    print(f'image file: {image_filename}')
    print(f'image size: {len(image)}')
    print(f'image url: {image_url}')

    # save the image
    with open(image_filename, 'wb') as f:
        f.write(image)


if __name__ == '__main__':
    # define command-line parameters
    parser = argparse.ArgumentParser(
        prog='OpenARK',
        description='OpenARK Python',
    )
    parser.add_argument(
        '--save_dir',
        type=str,
        help='a directory to save an image',
        default='.',
    )

    # parse command-line parameters
    args = parser.parse_args()
    ark = OpenArk()

    asyncio.run(upload_image(ark, args.save_dir))
