import json
import os
from pathlib import Path

# def stringify(entry):
#     try:
#         return str(entry)
#     except:
#         raise(ValueError(f"{entry} is not a number!"))

def get_json(path: str, id = None):
    with open(path, "r") as file:
        data = json.load(file)

        if id != None:

            if type(id) == str:
                try:
                    return data[str(id)]
                except:
                    raise(IndexError(f"Offer number {id} doesn't exist!"))
                
            elif type(id) == list:
                ids = {}
                for entry in id:
                    try:
                        ids[str(entry)] = data[str(entry)]
                    except:
                        raise(IndexError(f"Offer number {entry} doesn't exist!"))
                return ids
            
            else:
                id = str(id)
                return data[id]

        return data

path = Path.cwd() / "Updates" / "All offers" / "offerconfig.json"

print(get_json(path, [1]))