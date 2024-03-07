import argparse

from openark import OpenArk


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
    parser.add_argument(
        'filename',
        type=str,
        help='a file name to store data',
    )

    # parse command-line parameters
    args = parser.parse_args()
    ark = OpenArk()
    model = ark.get_model(args.model)

    pl = model.to_delta_polars()
    pl.write_csv(args.filename)
