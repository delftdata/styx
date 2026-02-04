from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

plot_operator = Operator("plot")
# key: movie_id


@plot_operator.register
async def write(ctx: StatefulFunction, plot: str):
    ctx.put(plot)
    return ctx.key
