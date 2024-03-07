import argparse
import asyncio
from pprint import pprint as print

from openark import OpenArk, OpenArkModel


async def execute_query(
    model: OpenArkModel,
    vector_column_name: str,
    query: str,
) -> None:
    # get lancedb table
    table = model.to_lancedb()

    # execute query
    df = table.search(
        query=query,
        vector_column_name=vector_column_name,
    ).to_polars()

    # show results
    print(df)


if __name__ == '__main__':
    # define command-line parameters
    parser = argparse.ArgumentParser(
        prog='OpenARK',
        description='OpenARK Python',
    )
    parser.add_argument(
        'model',
        type=str,
        help='a LanceDB model (table) to be executed',
    )
    parser.add_argument(
        'vector_column_name',
        type=str,
        help='a vector column name of a query',
    )
    parser.add_argument(
        'query',
        type=str,
        help='a SQL query to be executed',
    )

    # parse command-line parameters
    args = parser.parse_args()
    ark = OpenArk()
    model = ark.get_model(args.model)

    asyncio.run(execute_query(
        model,
        args.vector_column_name,
        args.query,
    ))
