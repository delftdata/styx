from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

text_operator = Operator("text")
# stateless


@text_operator.register
async def upload_text_2(ctx: StatefulFunction, text: str):
    ctx.call_remote_async(operator_name="compose_review",
                          function_name="upload_text",
                          key=ctx.key,
                          params=(text, ))
