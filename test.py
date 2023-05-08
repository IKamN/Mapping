# Define a dictionary with attribute names and values
my_dict = {"name": "John", "age": 30, "location": "New York", 'properties':{'one':[1,2,3], 'two':['asd', 'asda']}}

# Define a new class
class Person:
    def __init__(self, my_dict:dict):
        # Dynamically set attributes on the class based on the dictionary
        for key, value in my_dict.items():
            setattr(Person, key, value)

    def check(self):
        print([(key, value) for key, value in self.properties.items()])


# Create an instance of the new class
person = Person(my_dict)

# Access the attributes of the new class instance
# print(person.name)  # Output: John
# print(person.age)  # Output: 30
# print(person.location)  # Output: New York
print(person.check())