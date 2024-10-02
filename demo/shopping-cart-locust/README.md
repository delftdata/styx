To run the shopping cart demo first run from the top level of the project:

> ./scripts/start_shopping_cart_cluster.sh

This will start the cluster and the shopping cart gateway.

Then to init the system and run locust do:

>  ./demo/shopping-cart-locust/run_workload.sh

To use locust go to: `http://0.0.0.0:8089`

To close the cluster do:

> ./scripts/stop_shopping_cart_cluster.sh