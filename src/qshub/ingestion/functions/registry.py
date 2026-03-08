CHECKS_REGISTRY = {}

def register(name):
    def decorator(func):
        CHECKS_REGISTRY[name] = func
        return func
    return decorator


def get_func(name):
    return CHECKS_REGISTRY[name]