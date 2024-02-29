from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

order_operator = Operator('order')


class NotEnoughSpace(Exception):
    pass


@order_operator.register
async def create(ctx: StatefulFunction, flight_id: int, hotel_id: int, user_id: int):
    ctx.call_remote_async(operator_name='hotel',
                          function_name='reserve',
                          key=hotel_id)
    ctx.call_remote_async(operator_name='flight',
                          function_name='reserve',
                          key=flight_id)
    order_data: dict = {
        'FlightId': flight_id,
        'HotelId': hotel_id,
        'UserId': user_id
    }
    ctx.put(order_data)
    return "Reservation successful"
