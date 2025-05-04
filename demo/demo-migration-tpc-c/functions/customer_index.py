from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


customer_idx_operator = Operator('customer_idx',
                                 composite_key_hash_params=(0, ':'))
# Key -> w_id:d_id:c_last


class CustomerDoesNotExist(Exception):
    pass


@customer_idx_operator.register
async def insert(ctx: StatefulFunction, customer_idx: list):
    ctx.put(customer_idx)


@customer_idx_operator.register
async def pay(ctx: StatefulFunction, frontend_key, h_amount, d_id, w_id):
    index_data: list[dict[str, str]] = ctx.get()
    if index_data is None:
        raise CustomerDoesNotExist(f'Customer with id: {ctx.key} does not exist in the index')
    index = (len(index_data) - 1) // 2
    customer_key = index_data[index]
    ctx.call_remote_async(
        'customer',
        'pay',
        customer_key,
        # needed to get back the reply
        (frontend_key, h_amount, d_id, w_id)
    )
