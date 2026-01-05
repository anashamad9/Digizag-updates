import json
import os
from pathlib import Path

def form_json(values: dict, **kwargs):
    try:
        OFFER_ID = values["OFFER_ID"][0]
        GEO = values["GEO"][0]
        REVENUE_RATE = values["REVENUE_RATE"][0]
        CURRENCY_DIVISOR = values["CURRENCY_DIVISOR"][0]       # Convert SAR totals into target currency
        DAYS_BACK = values["DAYS_BACK"][0] # Limit optional historical window
        STATUS_DEFAULT = values["STATUS_DEFAULT"][0]
        DEFAULT_AFFILIATE_ID = values["DEFAULT_AFFILIATE_ID"][0]
        INPUT_RESOURCE = values["INPUT_RESOURCE"][0]
        SHEET = values["SHEET"][0]
        OUTPUT_FILENAME = values["OUTPUT_FILENAME"][0]
        DATE_FORMAT = values["DATE_FORMAT"][0]

        return {
            OFFER_ID:
            {
            "GEO": GEO,
            "REVENUE_RATE": REVENUE_RATE,
            "CURRENCY_DIVISOR": CURRENCY_DIVISOR,
            "DAYS_BACK": DAYS_BACK,
            "STATUS_DEFAULT": STATUS_DEFAULT,
            "DEFAULT_AFFILIATE_ID": DEFAULT_AFFILIATE_ID,
            "INPUT_RESOURCE": INPUT_RESOURCE,
            "SHEET": SHEET,
            "OUTPUT_FILENAME": OUTPUT_FILENAME,
            "DATE_FORMAT": DATE_FORMAT
            }
        }
    except:
        raise(ValueError("Missing Features."))

def insert_into_json(file_path, new_data):
    """
    Inserts new_data (dict or list item) into a JSON file.
    If the file doesn't exist, it creates one.
    If the JSON is an object, it updates it.
    If the JSON is a list, it appends to it.
    """

    # If file doesn't exist, create it with appropriate structure
    if not os.path.exists(file_path):
        # If new_data is a dict, start file as an object
        # If list item, start file as a list
        initial_data = new_data if isinstance(new_data, dict) else [new_data]
        with open(file_path, "w") as f:
            json.dump(initial_data, f, indent=4)
        return

    # Load existing data
    with open(file_path, "r") as f:
        try:
            data = json.load(f)
            print(data)
        except json.JSONDecodeError:
            # If file is empty or corrupted, reset it
            data = {}

    # Insert / append new data depending on structure
    if list(new_data.keys())[0] in data.keys():
        data[list(new_data.keys())[0]] = new_data[list(new_data.keys())[0]]
    else:
        if isinstance(data, dict) and isinstance(new_data, dict):
            data.update(new_data)
        elif isinstance(data, list):
            data.append(new_data)
        else:
            raise TypeError("Incompatible JSON structure and new_data type.")

    # Save updated data
    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)


path = Path.cwd()
path = path  / "Updates" / "All offers" / "offerconfig.json"
# C:\Users\lenovo\Documents\GitHub\Digizag-updates\Updates\All offers\offerconfig.json
# print(R)
# Example usage:
# insert_into_json(path, {"name": "Alice", "age": 25})

offer = {
    "OFFER_ID": [None, "str"],
    "GEO": [None, "str"],
    "REVENUE_RATE": [None, "float"],
    "CURRENCY_DIVISOR": [None, "float"],       # Convert SAR totals into target currency
    "DAYS_BACK": [None, "int"], # Limit optional historical window
    "STATUS_DEFAULT": [None, "str"],
    "DEFAULT_AFFILIATE_ID": [None, "str"],
    "INPUT_RESOURCE": [None, "str"],
    "SHEET": [None, "str"],
    "OUTPUT_FILENAME": [None, "str"],
    "DATE_FORMAT": [None, "str"]
}

# form_json(ID = "1234")
# or for list-based files:
# insert_into_json("list_data.json", {"id": 1, "item": "Apple"})

item = None

for key in offer.keys():
    item = input(f"Enter value for {key}: ")
    if item != '\\':
        if offer[key][1] == "int":
            offer[key][0] = int(item)
        if offer[key][1] == "float":
            offer[key][0] = float(item)
        if offer[key][1] == "str":
            offer[key][0] = item
    else:
        break

if item != '\\':
    insert_into_json(path, form_json(values=offer) )