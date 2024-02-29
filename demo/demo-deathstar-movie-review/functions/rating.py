from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

rating_operator = Operator('rating')
# stateless


@rating_operator.register
async def upload_rating_2(ctx: StatefulFunction, rating: int):
    ctx.call_remote_async(operator_name='compose_review',
                          function_name='upload_rating',
                          key=ctx.key,
                          params=(rating, ))
