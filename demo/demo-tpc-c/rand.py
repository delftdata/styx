# -----------------------------------------------------------------------
# Adapted from MongoDB-labs py-tpcc package:
# https://github.com/mongodb-labs/py-tpcc/blob/master/pytpcc/util/rand.py
# -----------------------------------------------------------------------
import random
from math import floor

from nurand import NURandC, make_for_load


SYLLABLES: list[str] = ["BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"]

nu_rand_var = make_for_load()


def nu_rand(a: int, x: int, y: int) -> int:
    """
    A non-uniform random number, as defined by TPC-C 2.1.6. (page 20).
    """
    assert x <= y

    if a == 255:
        c = nu_rand_var.c_last
    elif a == 1023:
        c = nu_rand_var.c_id
    elif a == 8191:
        c = nu_rand_var.order_line_item_id
    else:
        raise Exception(f"a = {a} is not a supported value")

    return (((number(0, a) | number(x, y)) + c) % (y - x + 1)) + x


def number(minimum, maximum):
    value = random.randint(minimum, maximum)
    assert minimum <= value <= maximum
    return value


def number_excluding(minimum: int, maximum: int, excluding: int) -> int:
    """
    An in the range [minimum, maximum], excluding excluding.
    """
    assert minimum < maximum
    assert minimum <= excluding <= maximum

    # Generate 1 less number than the range
    num = number(minimum, maximum - 1)

    # Adjust the numbers to remove excluding
    if num >= excluding:
        num += 1
    assert minimum <= num <= maximum and num != excluding
    return num


def fixed_point(decimal_places: int, minimum: float, maximum: float) -> float:
    assert decimal_places > 0
    assert minimum < maximum

    multiplier = 1
    for i in range(0, decimal_places):
        multiplier *= 10

    int_min = int(minimum * multiplier + 0.5)
    int_max = int(maximum * multiplier + 0.5)

    return float(number(int_min, int_max) / float(multiplier))


def select_unique_ids(num_unique: int, minimum: int, maximum: int) -> set:
    rows: set = set()
    for i in range(0, num_unique):
        index = None
        while index is None or index in rows:
            index = number(minimum, maximum)
        rows.add(index)
    assert len(rows) == num_unique
    return rows


def a_string(minimum_length: int, maximum_length: int) -> str:
    """
    A random alphabetic string with length in range [minimum_length, maximum_length].
    """
    return random_string(minimum_length, maximum_length, 'a', 26)


def n_string(minimum_length: int, maximum_length: int) -> str:
    """
    A random numeric string with length in range [minimum_length, maximum_length].
    """
    return random_string(minimum_length, maximum_length, '0', 10)


def random_string(minimum_length: int, maximum_length: int, base, num_characters: int) -> str:
    length = number(minimum_length, maximum_length)
    base_byte = ord(base)
    string = ""
    for i in range(length):
        string += chr(base_byte + number(0, num_characters - 1))
    return string


def make_last_name(num: int) -> str:
    """
    A last name as defined by TPC-C 4.3.2.3. Not actually random.
    """
    global SYLLABLES
    assert 0 <= num <= 999

    indices = [floor(num / 100), floor((num / 10)) % 10, num % 10]
    return "".join(map(lambda x: SYLLABLES[x], indices))


def make_random_last_name(max_cid: int) -> str:
    """
    A non-uniform random last name, as defined by TPC-C 4.3.2.3. The name will be limited to max_cid.
    """
    min_cid: int = 999

    if (max_cid - 1) < min_cid:
        min_cid = max_cid - 1

    return make_last_name(nu_rand(255, 0, min_cid))
