from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

ycsb_operator = Operator("ycsb")

# key (int): value tuple[10] (bytes 100)


@ycsb_operator.register
async def insert_batch(ctx: StatefulFunction, key_value_pairs: dict[any, any]):
    ctx.batch_insert(key_value_pairs)


@ycsb_operator.register
async def read(ctx: StatefulFunction):
    ctx.get()
    return ctx.key


@ycsb_operator.register
async def update(ctx: StatefulFunction):
    value: tuple = ctx.get()
    ctx.put(value)
    return ctx.key


@ycsb_operator.register
async def verify(ctx: StatefulFunction):
    """Fingerprint of the stored record (None if missing), for end-of-run state
    checks. Self-contained: it runs on the workers, where this module is absent."""
    import zlib

    value = ctx.get()
    return ctx.key, None if value is None else zlib.crc32("\x1f".join(value).encode())
