class DecoratorExample:
    def __init__(self, prefix):
        self.prefix = prefix
    
    def custom_decorator(self, value):
        def decorator(func):
            def wrapper(*args, **kwargs):
                print(f"{self.prefix} Decorator: Before function execution")
                result = func(*args, **kwargs)
                print(f"{self.prefix} Decorator: After function execution")
                return result
            return wrapper
        
        # Check if the value is a function or a variable
        if callable(value):
            # If it's a function, return the decorated function
            return decorator(value)
        else:
            # If it's a variable, return a decorator function
            return decorator

# Create an instance of the class
instance = DecoratorExample("Custom")

# Use the decorator with a function
@instance.custom_decorator
def my_function():
    print("Inside the function")

my_function()

# Use the decorator with a variable
@instance.custom_decorator("Variable")
def another_function():
    print("Inside another function")

another_function()


# ----------------------------------------------

def my_decorator(arg):
    def decorator(func):
        def wrapper(*args, **kwargs):
            print(f"Decorator: Before function execution with arg: {arg}")
            result = func(*args, **kwargs)
            print(f"Decorator: After function execution with arg: {arg}")
            return result
        return wrapper
    
    # Check if arg is a function or a string
    if callable(arg):
        return decorator(arg)
    else:
        def decorator_with_string(func):
            def wrapper(*args, **kwargs):
                print(f"Decorator: String argument: {arg}")
                result = func(*args, **kwargs)
                return result
            return wrapper
        return decorator_with_string

# Using the decorator with a function argument
@my_decorator("Custom String")
def my_function():
    print("Inside the function")

my_function()

# Using the decorator without a function argument
@my_decorator("Another String")
def another_function():
    print("Inside another function")

another_function()
