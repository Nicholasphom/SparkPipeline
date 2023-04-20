import pandas as pd
import os
import json


def extract_csv(file_path):
    try:
        print("Extracting CSV")
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print("Error Getting Dataset")

def extract_json(file_path):
    try:
        print("Extracting Json")
        df = pd.read_json(file_path)
        return df
    except Exception as e:
        print("Error Getting Dataset")
