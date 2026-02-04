import uuid

from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

unique_id_operator = Operator("unique_id")
# stateless


@unique_id_operator.register
async def upload_unique_id_2(ctx: StatefulFunction):
    review_id = uuid.uuid1().int >> 64
    ctx.call_remote_async(operator_name="compose_review",
                          function_name="upload_unique_id",
                          key=ctx.key,
                          params=(review_id, ))
