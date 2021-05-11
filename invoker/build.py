import os
import json

with open("invoker/tables.json", "w") as f:
    json.dump({"tables": [i.split(".")[0] for i in os.listdir("schemas")]}, f)
