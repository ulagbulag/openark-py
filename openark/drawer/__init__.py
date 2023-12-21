import ast
from typing import Any, Optional

from matplotlib.axes import Axes
import polars as pl
# from pprint import pprint as print
import seaborn as sns

from openark.drawer import timeseries

# Define all supported styles
_ALL_STYLES = {
    'timeseries': timeseries.draw,
}


def draw(lf: pl.LazyFrame, style: Optional[str] = None) -> Any:
    # check style argument
    style = style.strip()
    if style is None or len(style) == 0:
        return _draw_with_default_style(lf)

    # parse style argument
    style: ast.Module = ast.parse(style, mode='exec')
    if len(style.body) == 0:
        return _draw_with_default_style(lf)
    style_node = style.body[0].value
    if isinstance(style_node, ast.Name):
        style_func = ast.Call(ast.expr(), [], [])
        style_func.func = style_node
    elif isinstance(style_node, ast.Call):
        style_func = style_node
    else:
        print(type(style_node))
        raise ValueError('The style should be a function call')

    # check style function is supported
    style_func_name = style_func.func
    if not isinstance(style_func_name, ast.Name):
        raise ValueError('Currently the function name should be constant')
    if style_func_name.id not in _ALL_STYLES:
        raise ValueError(f'Unsupported style: {style_func_name.id}')

    # convert to style function implementation
    style_func_impl = ast.Subscript(
        value=ast.Name('_ALL_STYLES'),
        slice=ast.Constant(value=style_func_name.id),
    )
    style_func.func = style_func_impl

    # set lazy frame argument
    for kwarg in style_func.keywords:
        if kwarg.arg == 'lf':
            raise ValueError('Cannot use `lf` keyword argument directly')
    style_func.keywords.append(ast.keyword(
        arg='lf',
        value=ast.Name('__lf'),
    ))

    # eval
    return eval(ast.unparse(style_func), None, {
        '__lf': lf,
    })


def _draw_with_default_style(lf: pl.LazyFrame) -> Any:
    df = lf.collect()
    print(df)
    return df
