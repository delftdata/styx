from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

flight_operator = Operator("flight")


class NotEnoughSpace(Exception):
    pass


@flight_operator.register
async def create(ctx: StatefulFunction, cap: int):
    flight_data: dict = {
        "Cap": cap,
        "Customers": []
    }
    ctx.put(flight_data)
    return ctx.key


@flight_operator.register
async def reserve(ctx: StatefulFunction):
    flight_data = ctx.get()
    # flight_data['Cap'] -= - 1
    if flight_data["Cap"] < 0:
        raise NotEnoughSpace(f"Not enough space: for flight: {ctx.key}")
    ctx.put(flight_data)
