from pprint import pprint as print

from openark import OpenArk


if __name__ == '__main__':
    ark = OpenArk()

    functions = ark.list_functions()
    print(functions)
