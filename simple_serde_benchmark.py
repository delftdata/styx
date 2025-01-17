import sys

import ujson
import msgpack
import pickle
import json
import msgspec


from timeit import default_timer as timer


def ujson_ser(o):
    return ujson.dumps(o)


def ujson_de(b):
    return ujson.loads(b)


def pickle_ser(o, protocol=pickle.DEFAULT_PROTOCOL):
    return pickle.dumps(o, protocol=protocol)


def pickle_de(b):
    return pickle.loads(b)


def msgpack_ser(o):
    return msgpack.packb(o)


def msgpack_de(b):
    return msgpack.unpackb(b, strict_map_key=False)


def json_ser(o):
    return json.dumps(o)


def json_de(b):
    return json.loads(b)


def msgspec_ser(o):
    return msgspec.msgpack.encode(o)


def msgspec_de(b):
    return msgspec.msgpack.decode(b)


N = 10_000
d_size = 1_000
d = {i: i for i in range(d_size)}
print(f'Obj size {N*(sys.getsizeof(d) / 1_000_000)} MB')

print("UJson measurements")
start = timer()
for _ in range(N):
    B = ujson_ser(d)
end = timer()
t = round((end - start) * 1000, 4)
print(f"Ser time: {t} ms")

start = timer()
for _ in range(N):
    ujson_de(B)
end = timer()
t2 = round((end - start) * 1000, 4)
t += t2
print(f"De time: {t2} ms")
print(f"Total: {t} ms\n")

print("Pickle DEFAULT measurements")
start = timer()
for _ in range(N):
    B = pickle_ser(d)
end = timer()
t = round((end - start) * 1000, 4)
print(f"Ser time: {t} ms")

start = timer()
for _ in range(N):
    pickle_de(B)
end = timer()
t2 = round((end - start) * 1000, 4)
t += t2
print(f"De time: {t2} ms")
print(f"Total: {t} ms\n")

print("Pickle HIGHEST measurements")
start = timer()
for _ in range(N):
    B = pickle_ser(d, protocol=pickle.HIGHEST_PROTOCOL)
end = timer()
t = round((end - start) * 1000, 4)
print(f"Ser time: {t} ms")

start = timer()
for _ in range(N):
    pickle_de(B)
end = timer()
t2 = round((end - start) * 1000, 4)
t += t2
print(f"De time: {t2} ms")
print(f"Total: {t} ms\n")

print("Msgpack measurements")
start = timer()
for _ in range(N):
    B = msgpack_ser(d)
end = timer()
t = round((end - start) * 1000, 4)
print(f"Ser time: {t} ms")

start = timer()
for _ in range(N):
    msgpack_de(B)
end = timer()
t2 = round((end - start) * 1000, 4)
t += t2
print(f"De time: {t2} ms")
print(f"Total: {t} ms\n")

print("Json measurements")
start = timer()
for _ in range(N):
    B = json_ser(d)
end = timer()
t = round((end - start) * 1000, 4)
print(f"Ser time: {t} ms")

start = timer()
for _ in range(N):
    json_de(B)
end = timer()
t2 = round((end - start) * 1000, 4)
t += t2
print(f"De time: {t2} ms")
print(f"Total: {t} ms\n")

print("Msgspec measurements")
start = timer()
for _ in range(N):
    B = msgspec_ser(d)
end = timer()
t = round((end - start) * 1000, 4)
print(f"Ser time: {t} ms")

start = timer()
for _ in range(N):
    msgspec_de(B)
end = timer()
t2 = round((end - start) * 1000, 4)
t += t2
print(f"De time: {t2} ms")
print(f"Total: {t} ms\n")
