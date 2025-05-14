from typing import Any

from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

order_operator = Operator('order', n_partitions=4)


class OrderDoesNotExist(Exception):
    pass


@order_operator.register
async def create_order(ctx: StatefulFunction, user_id: str):
    ctx.put(
        {
            "paid": False,
            "items": {},
            "user_id": user_id,
            "total_cost": 0
        }
    )
    return ctx.key


@order_operator.register
async def batch_create(ctx: StatefulFunction, key_value_pairs: dict[Any, Any]):
    ctx.batch_insert(key_value_pairs)
    return "Batch insert successful"


@order_operator.register
async def find(ctx: StatefulFunction):
    value = ctx.get()
    if value is None:
        raise OrderDoesNotExist()
    return value


@order_operator.register
async def add_item(ctx: StatefulFunction, item_id: str, quantity: int):
    value = ctx.get()
    if value is None:
        raise OrderDoesNotExist()
    ctx.call_remote_async(
        operator_name='stock',
        function_name='find_for_order',
        key=item_id,
        params=(ctx.key, quantity)
    )


@order_operator.register
async def add_item_part_2(ctx: StatefulFunction, item_id: str, item_price: int, quantity: int):
    value = ctx.get()
    if value is None:
        raise OrderDoesNotExist()
    if item_id in value["items"]:
        value["items"][item_id] += quantity
    else:
        value["items"][item_id] = quantity
    value["total_cost"] += quantity * item_price
    ctx.put(value)
    return ctx.key


@order_operator.register
async def checkout(ctx: StatefulFunction):
    value = ctx.get()
    if value is None:
        raise OrderDoesNotExist()
    for item_id, quantity in value["items"].items():
        ctx.call_remote_async(
            operator_name='stock',
            function_name='remove_stock',
            key=item_id,
            params=(quantity, )
        )
    ctx.call_remote_async(
        operator_name='payment',
        function_name='remove_credit',
        key=value["user_id"],
        params=(value["total_cost"], )
    )
    value["paid"] = True
    ctx.put(value)
    return "Payment successful"
