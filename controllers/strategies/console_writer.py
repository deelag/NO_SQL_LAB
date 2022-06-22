import json


class ConsoleWriter:
    def __init__(self, *args, **kwargs):
        pass

    def write(self, data):
        for item in data:
            print(item)