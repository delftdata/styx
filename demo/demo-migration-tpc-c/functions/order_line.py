from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


order_line_operator = Operator('order_line',
                               composite_key_hash_params=(0, ':'))
# Key -> w_id:d_id:o_id:ol_number

class OrderLineDoesNotExist(Exception):
    pass


@order_line_operator.register
async def insert(ctx: StatefulFunction, order_line: dict):
    ctx.put(order_line)


@order_line_operator.register
async def get_order_line(ctx: StatefulFunction):
    order_line = ctx.get()
    if order_line is None:
        raise OrderLineDoesNotExist(f'OrderLine with key: {ctx.key} does not exist')
    return order_line
