from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

payment_operator = Operator('payment')


class NotEnoughCredit(Exception):
    pass


class UserDoesNotExist(Exception):
    pass


@payment_operator.register
async def create_user(ctx: StatefulFunction):
    ctx.put(
        {
            "credit": 0
        }
    )
    return ctx.key


@payment_operator.register
async def batch_create(ctx: StatefulFunction, key_value_pairs: dict[any, any]):
    ctx.batch_insert(key_value_pairs)
    return "Batch insert successful"


@payment_operator.register
async def find(ctx: StatefulFunction):
    value = ctx.get()
    if value is None:
        raise UserDoesNotExist()
    return value


@payment_operator.register
async def add_credit(ctx: StatefulFunction, amount: int):
    value = ctx.get()
    if value is None:
        raise UserDoesNotExist()
    value["credit"] += amount
    ctx.put(value)
    return value


@payment_operator.register
async def remove_credit(ctx: StatefulFunction, amount: int):
    value = ctx.get()
    if value is None:
        raise UserDoesNotExist()
    value["credit"] -= amount
    if value["credit"] < 0:
        raise NotEnoughCredit(f"User: {ctx.key} does not have enough credit")
    ctx.put(value)
