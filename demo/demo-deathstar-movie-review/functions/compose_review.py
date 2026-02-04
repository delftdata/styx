from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

compose_review_operator = Operator("compose_review")
# key: req_id


@compose_review_operator.register
async def upload_unique_id(ctx: StatefulFunction, review_id: str):
    compose_review_data = ctx.get()
    if compose_review_data is None:
        compose_review_data = {"review_id": review_id}
    else:
        compose_review_data["review_id"] = review_id
    ctx.put(compose_review_data)


@compose_review_operator.register
async def upload_user_id(ctx: StatefulFunction, user_id: str):
    compose_review_data = ctx.get()
    if compose_review_data is None:
        compose_review_data = {"userId": user_id}
    else:
        compose_review_data["userId"] = user_id
    ctx.put(compose_review_data)


@compose_review_operator.register
async def upload_movie_id(ctx: StatefulFunction, movie_id: str):
    compose_review_data = ctx.get()
    if compose_review_data is None:
        compose_review_data = {"movieId": movie_id}
    else:
        compose_review_data["movieId"] = movie_id
    ctx.put(compose_review_data)


@compose_review_operator.register
async def upload_rating(ctx: StatefulFunction, rating: int):
    compose_review_data = ctx.get()
    if compose_review_data is None:
        compose_review_data = {"rating": rating}
    else:
        compose_review_data["rating"] = rating
    ctx.put(compose_review_data)


@compose_review_operator.register
async def upload_text(ctx: StatefulFunction, text: str):
    compose_review_data = ctx.get()
    if compose_review_data is None:
        compose_review_data = {"text": text}
    else:
        compose_review_data["text"] = text
    ctx.put(compose_review_data)
