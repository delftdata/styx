from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


new_order_operator = Operator('new_order',
                          composite_key_hash_params=(0, ':'))
# Key -> w_id:d_id:d_next_o_id

@new_order_operator.register
async def insert(ctx: StatefulFunction, new_order_data: dict):
    ctx.put(new_order_data)
