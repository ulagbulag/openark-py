import argparse

from pprint import pprint as print

from openark import OpenArk


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='OpenARK',
        description='OpenARK Python',
    )
    parser.add_argument(
        'query',
        type=str,
        help='a SQL query to be executed',
    )

    args = parser.parse_args()
    ark = OpenArk()
    models = ark.get_global_namespace()

    df = models.sql(args.query).collect()
    print(df)
