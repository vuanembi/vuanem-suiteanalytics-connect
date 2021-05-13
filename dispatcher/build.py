import os
import json

with open("dispatcher/tables.json", "w") as f:
    tables = []
    for i in os.listdir("config"):
        with open(f"config/{i}", 'r') as c:
            if json.load(c).get('keys'):
                incre = True
            else:
                incre = False
            tables.append({"tables": i.split(".")[0], "incre": incre})
    json.dump(tables, f)
