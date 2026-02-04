from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

cast_info_operator = Operator("cast_info")
# Global state query


@cast_info_operator.register
async def write(ctx: StatefulFunction, info: dict):
    ctx.put(info)
    return ctx.key


@cast_info_operator.register
async def read(ctx: StatefulFunction, cast_ids: list[str]):
    return [cast_info for cast_info in ctx.data if cast_info["id"] in cast_ids]
