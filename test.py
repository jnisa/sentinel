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
        return decorator

# # Create an instance of the class
# instance = DecoratorExample("Custom")

# # Use the decorator with a function
# @instance.custom_decorator("Value")
# def my_function():
#     print("Inside the function")

# my_function()

# # # Use the decorator with a variable
# # @instance.custom_decorator("Variable")
# # def another_function():
# #     print("Inside another function")

# # another_function()


# ---

import unittest
from io import StringIO
import sys

class TestDecoratorExample(unittest.TestCase):

    def setUp(self):
        self.saved_stdout = sys.stdout
        sys.stdout = StringIO()

    def tearDown(self):
        sys.stdout = self.saved_stdout

    def test_custom_decorator(self):
        instance = DecoratorExample("Custom")

        @instance.custom_decorator("Value")
        def my_function():
            print("Inside the function")

        my_function()

        output = sys.stdout.getvalue()
        expected_output = "Custom Decorator: Before function execution\nInside the function\nCustom Decorator: After function execution\n"
        self.assertEqual(output, expected_output)

if __name__ == '__main__':
    unittest.main()
