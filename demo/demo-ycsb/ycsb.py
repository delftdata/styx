from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

ycsb_operator = Operator("ycsb")


class NotEnoughCredit(Exception):
    pass


@ycsb_operator.register
async def insert(ctx: StatefulFunction):
    value: int = 1_000_000
    ctx.put(value)
    return ctx.key


@ycsb_operator.register
async def insert_batch(ctx: StatefulFunction, key_value_pairs: dict[any, any]):
    ctx.batch_insert(key_value_pairs)


@ycsb_operator.register
async def read(ctx: StatefulFunction):
    value: int = ctx.get()
    return ctx.key, value


@ycsb_operator.register
async def update(ctx: StatefulFunction):
    new_value = ctx.get()
    new_value += 1
    ctx.put(new_value)
    return ctx.key, new_value


@ycsb_operator.register
async def update_t(ctx: StatefulFunction):
    new_value = ctx.get()
    new_value += 1
    ctx.put(new_value)


@ycsb_operator.register
async def transfer(ctx: StatefulFunction, key_b: str):
    value_a = ctx.get()

    ctx.call_remote_async(
        operator_name="ycsb",
        function_name="update_t",
        key=key_b
    )

    value_a -= 1

    if value_a < 0:
        raise NotEnoughCredit(f"Not enough credit for"
                              f" user: {ctx.key}")

    ctx.put(value_a)

    return ctx.key, value_a
