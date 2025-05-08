# 🛒 Styx Shopping Cart Example

This example demonstrates how to build a **shopping cart microservice deployment** using **Styx**.
It combines three Styx stateful operators — `order`, `payment`, and `stock` — orchestrated via a Sanic-based async REST API.

---

## Overview

The shopping cart microservice deployment example showcases:

- A **Sanic-based HTTP API** as the frontend interface
- A **StateflowGraph** with:
    - `order_operator`: creates orders and handles checkouts
    - `payment_operator`: manages user balances
    - `stock_operator`: maintains inventory and pricing
- Full **asynchronous event-driven coordination** between services using remote async calls

---

## Setup

1. Start the Styx Coordinator and Worker, and the Locust workload client following the demo [README](https://github.com/delftdata/styx/blob/main/demo/shopping-cart-locust/README.md).
This will start a Styx deployment and a Sanic HTTP API as the frontend interface.

2. Submit the dataflow graph:

```bash
POST /submit/<n_partitions>
```

---

## API Endpoints

For a detailed implementation of the Sanic HTTP API refer to [:octicons-arrow-right-24: app.py](https://github.com/delftdata/styx/blob/main/shopping-cart-demo/app.py){ .primary-link }

### 📦 Order

| Endpoint                           | Description                             |
|------------------------------------|-----------------------------------------|
| `POST /orders/create/<user_key>`  | Create a new order                      |
| `POST /orders/checkout/<order_id>`| Attempt to complete the order           |
| `POST /orders/addItem/<order>/<item>/<qty>` | Add item to an order           |
| `GET  /orders/find/<order_id>`    | Lookup order state                      |
| `POST /orders/batch_init/...`     | Bulk-create sample orders               |

### 💳 Payment

| Endpoint                               | Description                             |
|----------------------------------------|-----------------------------------------|
| `POST /payment/create_user`           | Register a new user                     |
| `POST /payment/add_funds/<id>/<amt>` | Add credit to a user                    |
| `GET  /payment/find_user/<id>`       | Fetch user credit info                  |
| `POST /payment/batch_init/...`       | Initialize many users at once           |

### 🏷️ Stock

| Endpoint                             | Description                             |
|--------------------------------------|-----------------------------------------|
| `POST /stock/item/create/<price>`   | Create a stock item                     |
| `POST /stock/add/<item>/<qty>`      | Add inventory                           |
| `GET  /stock/find/<item>`           | Lookup inventory and price              |
| `POST /stock/batch_init/...`        | Bulk insert stock items                 |

---

## Operator Logic

For the implementation of the Styx operator functions refer to [:octicons-arrow-right-24: functions](https://github.com/delftdata/styx/tree/main/shopping-cart-demo/functions){ .primary-link }


### Order Operator

- `create_order(user_id)` → initializes empty cart
- `add_item(item_id, qty)` → makes remote call to stock
- `checkout()` → removes credit and stock remotely
- `find()` → returns order state

### Payment Operator

- `create_user()` → zero credit
- `add_credit(amount)` → modifies user balance
- `remove_credit(amount)` → ensures no overdraft

### Stock Operator

- `create_item(price)` → zero stock
- `add_stock(amount)` → increase count
- `remove_stock(amount)` → raise error if insufficient
- `find_for_order()` → respond with price/availability

---

This example highlights how Styx enables object-oriented cloud programming through fine-grained async workflows that are fault-tolerant and distributed by design.
