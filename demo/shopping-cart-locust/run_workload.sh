#!/bin/bash

python demo/shopping-cart-locust/init_orders.py

locust -f demo/shopping-cart-locust/locustfile.py --host="localhost" --processes 2