import argparse
import asyncio
from pprint import pprint as print

from openark import OpenArk


async def call_function(
    ark: OpenArk,
    context: str,
    question: str,
) -> None:
    # define a function
    function_name = 'question-answering'
    function = await ark.get_function(function_name)

    # make an input value
    input = {
        "context": context,
        "question": question,
    }

    # call the function and get a response
    output = await function(input)

    # print the output values
    print(output)


if __name__ == '__main__':
    # define command-line parameters
    parser = argparse.ArgumentParser(
        prog='OpenARK',
        description='OpenARK Python',
    )
    parser.add_argument(
        '--context',
        type=str,
        help='a long context',
    )
    parser.add_argument(
        '--question',
        type=str,
        help='a question',
    )

    # parse command-line parameters
    args = parser.parse_args()
    ark = OpenArk()

    asyncio.run(call_function(ark, args.context, args.question))
