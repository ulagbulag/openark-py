
from matplotlib.axes import Axes
import polars as pl
import seaborn as sns


def draw(
    *,
    lf: pl.LazyFrame,
    x: str = '__timestamp',
    y: str,
    hue: str,
) -> Axes:
    sns.set_theme(style='darkgrid')

    return sns.lineplot(
        data=lf.collect().to_pandas(),
        x=x,
        y=y,
        hue=hue,
    )
