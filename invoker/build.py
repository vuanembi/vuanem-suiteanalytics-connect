import os
import json

with open("invoker/tables.json", "w") as f:
    json.dump({"tables": [i.split(".")[0] for i in os.listdir("vuanem_ns/schemas")]}, f)
