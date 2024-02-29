from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph

from functions.history import history_operator
from functions.item import item_operator
from functions.new_order import new_order_operator
from functions.order import order_operator
from functions.order_line import order_line_operator
from functions.stock import stock_operator
from functions.warehouse import warehouse_operator
from functions.district import district_operator
from functions.customer import customer_operator
from functions.new_order_txn import new_order_txn_operator
from functions.payment_txn import payment_txn_operator
from functions.customer_index import customer_idx_operator

####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('tpcc_benchmark', operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
# REGISTER OPERATOR FUNCTIONS ########################################################################################
####################################################################################################################
g.add_operators(customer_operator, district_operator, history_operator, item_operator, new_order_operator,
                order_operator, order_line_operator, stock_operator, warehouse_operator,
                new_order_txn_operator, customer_idx_operator, payment_txn_operator)
