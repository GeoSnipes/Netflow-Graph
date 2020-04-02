from time import time


def timeme(func):
    def wrapper(*args, **kwargs):
        starttime = time()
        return func(*args, **kwargs), round(time() - starttime,7)
    return wrapper

