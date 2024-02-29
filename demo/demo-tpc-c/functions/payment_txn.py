from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


payment_txn_operator = Operator('payment_txn')


def send_output(front_end_metadata):
    if (front_end_metadata['warehouse_data'] is not None and
            front_end_metadata['district_data'] is not None and
            front_end_metadata['customer_data'] is not None):
        return True
    return False


def pack_response(ctx, front_end_metadata):
    # Adjust the total for the discount
    warehouse_data = front_end_metadata['warehouse_data']
    district_data = front_end_metadata['district_data']
    customer_data = front_end_metadata['customer_data']

    # Concatenate w_name, four spaces, d_name
    h_data = f"{warehouse_data['W_NAME']}    {district_data['D_NAME']}"

    # ----------------------
    # Insert History Query
    # ----------------------
    w_id = front_end_metadata['W_ID']
    d_id = front_end_metadata['D_ID']
    c_id = customer_data['C_ID']

    history_key = f'{w_id}:{d_id}:{c_id}'
    history_params = {
        'H_C_ID': c_id,
        'H_C_D_ID': front_end_metadata['C_D_ID'],
        'H_C_W_ID': front_end_metadata['C_W_ID'],
        'H_D_ID': d_id,
        'H_W_ID': w_id,
        'H_DATE': front_end_metadata['H_DATE'],
        'H_AMOUNT': front_end_metadata['H_AMOUNT'],
        'H_DATA': h_data,
    }

    ctx.call_remote_async(
        'history',
        'insert',
        history_key,
        (history_params, )
    )

    # TPC-C 2.5.3.3: Must display the following fields:
    # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY,
    # W_STATE, W_ZIP, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP,
    # C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE,
    # C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,
    # C_BALANCE, the first 200 characters of C_DATA
    # (only if C_CREDIT = "BC"), H_AMOUNT, and H_DATE.
    # Hand back all the warehouse, district, and customer data
    return warehouse_data, district_data, customer_data


@payment_txn_operator.register
async def get_customer(ctx: StatefulFunction, customer_data: dict):
    # --------------------
    # Get response from the district entity
    # --------------------
    front_end_metadata = ctx.get()
    front_end_metadata['customer_data'] = customer_data
    ctx.put(front_end_metadata)
    if send_output(front_end_metadata):
        response = pack_response(ctx, front_end_metadata)
        return response


@payment_txn_operator.register
async def get_warehouse(ctx: StatefulFunction, warehouse_data: dict):
    # --------------------
    # Get response from the warehouse entity
    # --------------------
    front_end_metadata = ctx.get()
    front_end_metadata['warehouse_data'] = warehouse_data
    ctx.put(front_end_metadata)
    if send_output(front_end_metadata):
        response = pack_response(ctx, front_end_metadata)
        return response


@payment_txn_operator.register
async def get_district(ctx: StatefulFunction, district_data: dict):
    # --------------------
    # Get response from the district entity
    # --------------------
    front_end_metadata = ctx.get()
    front_end_metadata['district_data'] = district_data
    ctx.put(front_end_metadata)
    if send_output(front_end_metadata):
        response = pack_response(ctx, front_end_metadata)
        return response


@payment_txn_operator.register
async def payment(ctx: StatefulFunction, params: dict):
    # Initialize transaction properties
    w_id: str = params['W_ID']
    d_id: str = params['D_ID']
    h_amount: float = params['H_AMOUNT']
    c_w_id: str = params['C_W_ID']
    c_d_id: str = params['C_D_ID']
    c_id: str = params['C_ID']
    c_last: str = params['C_LAST']
    h_date: str = params['H_DATE']

    # Init metadata
    init_data = {
        'W_ID': w_id,
        'D_ID': d_id,
        'C_ID': c_id,
        'C_W_ID': c_w_id,
        'C_D_ID': c_d_id,
        'H_DATE': h_date,
        'H_AMOUNT': h_amount,
        'warehouse_data': None,
        'district_data': None,
        'customer_data': None
    }

    ctx.put(init_data)

    if c_id is not None:
        # --------------------------
        # Get Customer By ID Query
        # --------------------------
        customer_key = f'{w_id}:{d_id}:{c_id}'
        ctx.call_remote_async(
            'customer',
            'pay',
            customer_key,
            # needed to get back the reply
            (ctx.key, h_amount, d_id, w_id)
        )
    else:
        # ----------------------------------
        # Get Customers By Last Name Query
        # ----------------------------------
        customer_idx_key = f'{c_w_id}:{c_d_id}:{c_last}'
        ctx.call_remote_async(
            'customer_idx',
            'pay',
            customer_idx_key,
            # needed to get back the reply
            (ctx.key, h_amount, d_id, w_id)
        )

    # --------------------
    # Update payment in warehouse and district
    # --------------------
    ctx.call_remote_async(
        'warehouse',
        'pay',
        w_id,
        (ctx.key, h_amount)
    )
    district_key = f'{w_id}:{d_id}'
    ctx.call_remote_async(
        'district',
        'pay',
        district_key,
        (ctx.key, h_amount)
    )
