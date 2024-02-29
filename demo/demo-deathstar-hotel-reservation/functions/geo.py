from geopy.distance import distance

from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

geo_operator = Operator('geo')


@geo_operator.register
async def create(ctx: StatefulFunction, lat: float, lon: float):
    geo_data: dict = {
        "Plat": lat,
        "Plon": lon
    }
    ctx.put(geo_data)
    return ctx.key


@geo_operator.register
async def nearby(ctx: StatefulFunction, lat: float, lon: float, in_date: int, out_date: int):
    distances = {}
    for key, point in ctx.data.items():
        distance_km = distance((lat, lon), (point["Plat"], point["Plon"])).km
        if distance_km < 10:
            distances[key] = distance_km
    distances = {k: v for k, v in sorted(distances.items(), key=lambda item: item[1])}
    res = {"HotelIds": [k for k in list(distances)[:5]]}
    ctx.call_remote_async(operator_name='rate',
                          function_name='get_rates',
                          key=ctx.key,
                          params=(res, in_date, out_date))
