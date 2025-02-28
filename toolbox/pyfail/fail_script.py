class CustomException(Exception):
    pass


def function_a():
    function_b()


def function_b():
    function_c()


def function_c():
    raise CustomException("Intentional custom failure")


if __name__ == "__main__":
    function_a()
