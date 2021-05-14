import os
import json

tables = []
for i in os.listdir("config"):
    with open(f"config/{i}", "r") as c:
        if json.load(c).get("keys"):
            incre = True
        else:
            incre = False
        tables.append({"tables": i.split(".")[0], "incre": incre})

with open("dispatcher/tables.json", "w") as f:
    json.dump(tables, f)
