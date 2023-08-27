def custom_decorator(func):
    def wrapper(*args, **kwargs):
        print("Before the function is called")
        result = func(*args, **kwargs)
        print(f"Result: {result}")
        print("After the function is called")
        return result
    return wrapper

@custom_decorator
def my_function(x, y):
    return x + y

result = my_function(3, 5)
# print("Result:", result)