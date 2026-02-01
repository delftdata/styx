from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

movie_id_operator = Operator("movie_id")
# key: title


@movie_id_operator.register
async def register_movie_id(ctx: StatefulFunction, movie_id: str):
    ctx.put({"movieId": movie_id})
    return ctx.key


@movie_id_operator.register
async def upload_movie(ctx: StatefulFunction, req_id: str, rating: int):
    item_data = ctx.get()
    if item_data is not None and "movieId" in item_data:
        ctx.call_remote_async(operator_name="compose_review",
                              function_name="upload_movie_id",
                              key=req_id,
                              params=(item_data["movieId"], ))
    else:
        ctx.call_remote_async(operator_name="rating",
                              function_name="upload_rating_2",
                              key=req_id,
                              params=(rating, ))
