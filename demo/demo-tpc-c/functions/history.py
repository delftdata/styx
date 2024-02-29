from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


history_operator = Operator('history')


class HistoryDoesNotExist(Exception):
    pass


@history_operator.register
async def insert(ctx: StatefulFunction, history: dict):
    ctx.put(history)


@history_operator.register
async def insert_batch(ctx: StatefulFunction, key_value_pairs: dict[any, any]):
    ctx.batch_insert(key_value_pairs)


@history_operator.register
async def get_history(ctx: StatefulFunction):
    history = ctx.get()
    if history is None:
        raise HistoryDoesNotExist(f'History with key: {ctx.key} does not exist')
    return history
