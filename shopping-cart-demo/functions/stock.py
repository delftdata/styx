from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

stock_operator = Operator('stock')


class NotEnoughStock(Exception):
    pass


class ItemDoesNotExist(Exception):
    pass


@stock_operator.register
async def create_item(ctx: StatefulFunction, price: int):
    ctx.put(
        {
            "stock": 0,
            "price": price
        }
    )
    return ctx.key


@stock_operator.register
async def batch_create(ctx: StatefulFunction, key_value_pairs: dict[any, any]):
    ctx.batch_insert(key_value_pairs)
    return "Batch insert successful"


@stock_operator.register
async def find(ctx: StatefulFunction):
    value = ctx.get()
    if value is None:
        raise ItemDoesNotExist()
    return value


@stock_operator.register
async def find_for_order(ctx: StatefulFunction, order_id: str, quantity: int):
    value = ctx.get()
    if value is None:
        raise ItemDoesNotExist()
    ctx.call_remote_async(
        operator_name='order',
        function_name='add_item_part_2',
        key=order_id,
        params=(ctx.key, value["price"], quantity)
    )


@stock_operator.register
async def add_stock(ctx: StatefulFunction, amount: int):
    value = ctx.get()
    if value is None:
        raise ItemDoesNotExist()
    value["stock"] += amount
    ctx.put(value)
    return ctx.key


@stock_operator.register
async def remove_stock(ctx: StatefulFunction, amount: int):
    value = ctx.get()
    if value is None:
        raise ItemDoesNotExist()
    value["stock"] -= amount
    if value["stock"] < 0:
        raise NotEnoughStock(f"Item: {ctx.key} does not have enough stock")
    ctx.put(value)
