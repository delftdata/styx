# -----------------------------------------------------------------------
# Adapted from MongoDB-labs py-tpcc package:
# https://github.com/mongodb-labs/py-tpcc/blob/master/pytpcc/util/nurand.py
# -----------------------------------------------------------------------
import random


class NURandC(object):
    def __init__(self, c_last, c_id, order_line_item_id):
        self.c_last: str = c_last
        self.c_id: int = c_id
        self.order_line_item_id: int = order_line_item_id


def make_for_load() -> NURandC:
    """
    Create random NURand constants, appropriate for loading the database.
    """
    c_last: int = random.randint(0, 255)
    c_id: int = random.randint(0, 1023)
    order_line_item_id: int = random.randint(0, 8191)
    return NURandC(c_last, c_id, order_line_item_id)


def valid_c_run(c_run: int, c_load: int) -> bool:
    """
    Returns true if the cRun value is valid for running. See TPC-C 2.1.6.1 (page 20)
    """
    c_delta: int = abs(c_run - c_load)
    return 65 <= c_delta <= 119 and c_delta != 96 and c_delta != 112


def make_for_run(load_c: NURandC) -> NURandC:
    """
    Create random NURand constants for running TPC-C.
    TPC-C 2.1.6.1. (page 20) specifies the valid range for these constants.
    """
    c_run: int = random.randint(0, 255)

    while not valid_c_run(c_run, load_c.c_last):
        c_run = random.randint(0, 255)

    assert valid_c_run(c_run, load_c.c_last)

    c_id = random.randint(0, 1023)
    order_line_item_id = random.randint(0, 8191)
    return NURandC(c_run, c_id, order_line_item_id)
