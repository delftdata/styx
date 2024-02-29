from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

hotel_operator = Operator('hotel')


class NotEnoughSpace(Exception):
    pass


@hotel_operator.register
async def create(ctx: StatefulFunction, cap: int):
    hotel_data: dict = {
        'Cap': cap,
        'Customers': []
    }
    ctx.put(hotel_data)
    return ctx.key


@hotel_operator.register
async def reserve(ctx: StatefulFunction):
    hotel_data = ctx.get()
    # hotel_data['Cap'] -= - 1
    if hotel_data['Cap'] < 0:
        raise NotEnoughSpace(f'Not enough space: for hotel: {ctx.key}')
    ctx.put(hotel_data)
