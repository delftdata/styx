from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

frontend_operator = Operator('frontend')


@frontend_operator.register
async def compose(ctx: StatefulFunction, username, title, rating, text):
    ctx.call_remote_async(operator_name='unique_id',
                          function_name='upload_unique_id_2',
                          key=ctx.key)
    ctx.call_remote_async(operator_name='user',
                          function_name='upload_user',
                          key=username,
                          params=(ctx.key, ))
    ctx.call_remote_async(operator_name='movie_id',
                          function_name='upload_movie',
                          key=title,
                          params=(ctx.key, rating))
    ctx.call_remote_async(operator_name='text',
                          function_name='upload_text_2',
                          key=ctx.key,
                          params=(text, ))
    return "Compose Success"
