import argparse
import asyncio
import os
from pprint import pprint as print

from openark import OpenArk


# 본 코드는 call_function.py를 활용한 간단한 예제입니다. pub-nats-minio-face-detection-openark.py에서 실행하는 구조입니다.

async def publish_image(
    ark: OpenArk,
    filename: str, # 파이썬 명령줄 인수를 위한 변수 선언 부분
    function_name: str 
) -> None:
    # define a model
    model = await ark.get_model_channel(function_name) # function_name은 NATS의  jubject name처럼 취급.

    # check file
    if not os.path.exists(filename):
        raise ValueError(f"Image file {filename:?} not found!")

    # load payload files
    payload_image_key = 'detect-image.jpg'
    payloads = {
        f'{payload_image_key}': open(filename, 'rb').read(),
    }
    # 이미지 파일의 경우 위와 같은 포맷으로 구성이 가능.
    # 이 때, 해당 예제에서는 file을 로컬에 저장하였지만, 메모리에 담아서 보낼 수도 있다. 파일의 타입은 bytes
    # payload_image_key는 실제로 데이터가 저장되는 위치에서 적용되는 이름이다.

    # make an input value
    input = {
        "images": [f"@data:image,{payload_image_key}"],
    }

    # publish image data into the model
    await model.publish(
        input,
        payloads=payloads,
    )


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
    parser.add_argument(  # function_name에 대한 인수를 추가
        '--function_name',
        type=str,
        default='face-detection-sd',  # 기본값 지정
        help='function name to call',
    )

    # parse command-line parameters
    args = parser.parse_args()
    ark = OpenArk()

    asyncio.run(publish_image(ark, args.filename, args.function_name))

