from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph

from functions.compose_review import compose_review_operator
from functions.movie_id import movie_id_operator
from functions.movie_info import movie_info_operator
from functions.plot import plot_operator
from functions.rating import rating_operator
from functions.text import text_operator
from functions.unique_id import unique_id_operator
from functions.user import user_operator
from functions.frontend import frontend_operator

####################################################################################################################
# DECLARE A STATEFLOW GRAPH ########################################################################################
####################################################################################################################
g = StateflowGraph('deathstar_movie_review', operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
g.add_operators(compose_review_operator,
                movie_id_operator,
                movie_info_operator,
                plot_operator,
                rating_operator,
                text_operator,
                unique_id_operator,
                user_operator,
                frontend_operator)
