import argparse

from openark import OpenArk


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='OpenARK',
        description='OpenARK Python',
    )
    parser.add_argument(
        'model',
        help='model name',
    )
    parser.add_argument(
        'filename',
        help='a file name to store data',
    )

    args = parser.parse_args()
    ark = OpenArk()
    model = ark.get_model(args.model)

    pl = model.to_polars()
    pl.write_csv(args.filename)
