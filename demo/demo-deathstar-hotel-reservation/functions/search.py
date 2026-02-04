from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

search_operator = Operator("search")


class NotEnoughSpace(Exception):
    pass


@search_operator.register
async def nearby(ctx: StatefulFunction, lat: int, lon: int, in_date: int, out_date: int):
    ctx.call_remote_async(operator_name="geo",
                          function_name="nearby",
                          key=ctx.key,
                          params=(lat, lon, in_date, out_date))
