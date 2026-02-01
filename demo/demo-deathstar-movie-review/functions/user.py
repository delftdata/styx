from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

user_operator = Operator("user")
# key: username


@user_operator.register
async def register_user(ctx: StatefulFunction, user_data: dict):
    ctx.put(user_data)
    return ctx.key


@user_operator.register
async def upload_user(ctx: StatefulFunction, req_id: str):
    user_data = ctx.get()
    ctx.call_remote_async(operator_name="compose_review",
                          function_name="upload_user_id",
                          key=req_id,
                          params=(user_data["userId"], )
                          )
