from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


order_operator = Operator('order',
                          composite_key_hash_params=(0, ':'))
# Key -> w_id:d_id:d_next_o_id

class OrderDoesNotExist(Exception):
    pass


@order_operator.register
async def insert(ctx: StatefulFunction, order: dict):
    ctx.put(order)


@order_operator.register
async def get_order(ctx: StatefulFunction):
    order = ctx.get()
    if order is None:
        raise OrderDoesNotExist(f'Order with key: {ctx.key} does not exist')
    return order
