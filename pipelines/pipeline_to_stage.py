import pandas as pd
from extract_functions import extraction
class StagePipeline:
    def __init__(self,metamodel): 
        self.ExtractClass = None
        self.Backend = None
        self.Metamodel = metamodel
        self.dataset = None
    
    def extract_data(self,filepath):
        try:
            self.dataset = extraction.extract_csv(filepath)

        except Exception as e:
            print(f"Error in Extraxting CSV from {filepath}, pipeline will not run!")



