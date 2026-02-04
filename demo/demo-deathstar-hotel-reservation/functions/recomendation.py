from geopy.distance import distance
from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

recommendation_operator = Operator("recommendation")


class WrongRecommendationOption(Exception):
    pass


@recommendation_operator.register
async def create(ctx: StatefulFunction, lat: float, lon: float, rate: float, rate_inc: float):
    rec_data: dict = {
        "HLat": lat,
        "HLon": lon,
        "HRate": rate,
        "HPrice": rate_inc
    }
    ctx.put(rec_data)
    return ctx.key


@recommendation_operator.register
async def get_recommendations(ctx: StatefulFunction, requirement: str, lat: float, lon: float):
    rec_d = ctx.data
    if requirement == "dis":
        p1 = (lat, lon)
        distances = {hotel_key: distance(p1, (hotel["HLat"], hotel["HLon"])) / 1000
                     for hotel_key, hotel in rec_d.items()}
        min_dist = min(distances.values())
        res = [k for k, v in distances.items() if v == min_dist]
    elif requirement == "rate":
        max_rate = max(rec_d, key=lambda x: rec_d[x]["HRate"])
        res = [k for k, v in rec_d.items() if v["HRate"] == max_rate]
    elif requirement == "price":
        min_price = min(rec_d, key=lambda x: rec_d[x]["HPrice"])
        res = [k for k, v in rec_d.items() if v["HPrice"] == min_price]
    else:
        raise WrongRecommendationOption(f"No such requirement: {requirement}")
    return res
