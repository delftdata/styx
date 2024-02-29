from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

rate_operator = Operator('rate')


class NotEnoughSpace(Exception):
    pass


@rate_operator.register
async def create(ctx: StatefulFunction, code: str, in_date: str, out_date: str, room_type: dict):
    rate_data: dict = {
        "code": code,
        "Indate": in_date,
        "Outdate": out_date,
        "RoomType": room_type
    }
    ctx.put(rate_data)
    return ctx.key


@rate_operator.register
async def get_rates(ctx: StatefulFunction, hotel_ids: list, in_date: int, out_date: int):
    res_plans = [hotel_id for hotel_id in hotel_ids if hotel_id in ctx.data]
    return res_plans
