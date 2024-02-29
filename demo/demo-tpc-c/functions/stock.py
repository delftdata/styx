from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


stock_operator = Operator('stock')


class StockDoesNotExist(Exception):
    pass


@stock_operator.register
async def insert(ctx: StatefulFunction, stock: dict):
    ctx.put(stock)


@stock_operator.register
async def insert_batch(ctx: StatefulFunction, key_value_pairs: dict[any, any]):
    ctx.batch_insert(key_value_pairs)


@stock_operator.register
async def get_stock(ctx: StatefulFunction):
    stock = ctx.get()
    return stock


@stock_operator.register
async def update_stock(ctx: StatefulFunction,
                       frontend_key,
                       index,
                       w_id,
                       d_id,
                       i_w_id,
                       o_entry_d,
                       i_qty,
                       d_next_o_id,
                       i_name,
                       i_price,
                       i_brand_generic):
    # --------------------
    # Update Stock Query
    # --------------------
    stock_data = ctx.get()
    if stock_data is None:
        raise StockDoesNotExist(f'Stock with key: {ctx.key} does not exist')
    stock_data['S_YTD'] += i_qty
    if stock_data['S_QUANTITY'] >= i_qty + 10:
        stock_data['S_QUANTITY'] -= i_qty
    else:
        stock_data['S_QUANTITY'] = stock_data['S_QUANTITY'] + 91 - i_qty
    stock_data['S_ORDER_CNT'] += 1

    if i_w_id != w_id:
        stock_data['S_REMOTE_CNT'] += 1
    ctx.put(stock_data)

    if i_brand_generic and stock_data['S_DATA'].find("original") != -1:
        brand_generic = 'B'
    else:
        brand_generic = 'G'

    # Transaction profile states to use "ol_quantity * i_price"
    ol_amount = i_qty * i_price

    ctx.call_remote_async(
        'new_order_txn',
        'get_item_with_stock',
        frontend_key,
        (i_name, i_price, ol_amount, stock_data['S_QUANTITY'], brand_generic)
    )
    # -------------------------------
    # Insert Order Item Information
    # -------------------------------
    ol_number = index + 1
    d_next_o_id += 1

    if len(str(d_id)) == 1:
        s_dist_xx = stock_data[f'S_DIST_0{d_id}']
    else:
        s_dist_xx = stock_data[f'S_DIST_{d_id}']

    order_line_key = f'{w_id}:{d_id}:{d_next_o_id}:{ol_number}'
    order_line_params = {
        'OL_O_ID': ctx.key,
        'OL_SUPPLY_W_ID': i_w_id,
        'OL_DELIVERY_D': o_entry_d,
        'OL_QUANTITY': i_qty,
        'OL_AMOUNT': ol_amount,
        'OL_DIST_INFO': s_dist_xx
    }
    ctx.call_remote_async(
        'order_line',
        'insert',
        order_line_key,
        (order_line_params,)
    )
