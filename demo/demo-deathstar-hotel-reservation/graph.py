from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph

from functions.geo import geo_operator
from functions.flight import flight_operator
from functions.hotel import hotel_operator
from functions.order import order_operator
from functions.rate import rate_operator
from functions.recomendation import recommendation_operator
from functions.search import search_operator
from functions.user import user_operator

####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('deathstar_hotel_reservations', operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
g.add_operators(flight_operator,
                geo_operator,
                hotel_operator,
                order_operator,
                rate_operator,
                recommendation_operator,
                search_operator,
                user_operator)
