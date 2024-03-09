# x กับ y เราเรียกว่า args หรือ arguments
def add(x, y):
    print(f"X: {x} and Y: {y}")
    return x + y


results = add(1, 2)
print(f"Results: {results}")

# kwargs หรือ keyword arguments
def add2(x=0, y=0):
    print(f"X: {x} and Y: {y}")
    return x + y

results = add2(1, 2)
print(f"Results: {results}")

results = add2()
print(f"Results: {results}")

results = add2(x=10)
print(f"Results: {results}")

results = add2(y=5)
print(f"Results: {results}")

def add3(z, x=0, y=0):
    print(f"X: {x} and Y: {y} and Z: {z}")
    return x + y + z

results = add3(10, 5)
print(f"Results: {results}")


def add4(*args):
    print(args)
    print(args[3])

add4(1, 2, 3, 4, 5, 6, 7, 8)

def add5(**kwargs):
    print(kwargs)
    print(kwargs["z"])

add5(x=1, y=2, z=3, a=4, b=5, c=6, d=7, e=8)

def add6(x, y, **context):
    print(x, y)
    print(context)

add6(x=1, y=2, z=3, a=4, b=5, c=6, d=7, e=8)
add6(1, 2, z=3, a=4, b=5, c=6, d=7, e=8)