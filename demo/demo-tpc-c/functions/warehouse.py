from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


warehouse_operator = Operator('warehouse')
# Key -> w_id

class WHDoesNotExist(Exception):
    pass


@warehouse_operator.register
async def insert(ctx: StatefulFunction, warehouse: dict):
    ctx.put(warehouse)


@warehouse_operator.register
async def get_warehouse(ctx: StatefulFunction, frontend_key):
    warehouse_data = ctx.get()
    if warehouse_data is None:
        raise WHDoesNotExist(f'Warehouse with key: {ctx.key} does not exist')
    ctx.call_remote_async(
        'new_order_txn',
        'get_warehouse',
        frontend_key,
        (warehouse_data, )
    )


@warehouse_operator.register
async def pay(ctx: StatefulFunction, frontend_key, h_amount):
    warehouse_data = ctx.get()
    if warehouse_data is None:
        raise WHDoesNotExist(f'Warehouse with key: {ctx.key} does not exist')
    warehouse_data['W_YTD'] = float(warehouse_data['W_YTD']) + h_amount
    ctx.put(warehouse_data)
    ctx.call_remote_async(
        'payment_txn',
        'get_warehouse',
        frontend_key,
        (warehouse_data, )
    )
