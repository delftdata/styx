from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


history_operator = Operator('history',
                            composite_key_hash_params=(0, ':'))
# Key -> w_id:d_id:c_id

class HistoryDoesNotExist(Exception):
    pass


@history_operator.register
async def insert(ctx: StatefulFunction, history: dict):
    ctx.put(history)


@history_operator.register
async def get_history(ctx: StatefulFunction):
    history = ctx.get()
    if history is None:
        raise HistoryDoesNotExist(f'History with key: {ctx.key} does not exist')
    return history
