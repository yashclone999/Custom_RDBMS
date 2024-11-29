import time
from QueryProcessor import QueryProcessor
from DbStorage import DbStorage
import pandas as pd
import numpy as np
import os
import csv
import psutil

class QueryParser:

    def __init__(self) -> None:
        self.queryProcessor = None

    def tokenize(self, query):
        dict = {}
        lines = query.strip().split('@')
        for line in lines:
            try:
                if len(line) == 0 or line[0] == '\n': continue
                tokens = line.strip().split("=>")
                dict[tokens[0].strip()] = tokens[1].strip()
            except:
                assert False
        return dict
    
    def process(self, dict):
        '''
        {
            "create" : "table name",
            "columns" : col type size, col type size ...,
            "keys" : key, key, ...,
            "indexes" : in, in, ...
        }
        '''
        if "create" in dict:
            if self.queryProcessor is None:
                self.DbPrompt()
                return
            if self.isValid(dict,['create','columns','constraint']): self.queryProcessor.DDL(dict)
        elif "use" in dict:
            if self.queryProcessor is None:
                self.DbPrompt()
                return
            if self.isValid(dict,['use', 'combine', 'filter', 'categorize', 'project', 'sort', 'save']): self.queryProcessor.DML(dict)
        elif "populate" in dict:
            if self.queryProcessor is None:
                self.DbPrompt()
                return
            #  same order as colums in metadata while creating table
            if self.isValid(dict,['populate','values']): self.queryProcessor.insert(dict)
        elif "update" in dict:
            if self.queryProcessor is None:
                self.DbPrompt()
                return
            #  same order as colums in metadata while creating table
            if self.isValid(dict,['update','values','filter']): self.queryProcessor.update(dict)
        elif "delete" in dict:
            if self.queryProcessor is None:
                self.DbPrompt()
                return
            #  same order as colums in metadata while creating table
            if self.isValid(dict,['delete','filter']): self.queryProcessor.delete(dict)
        elif "database" in dict:
            # need to set DB path and object
            CWD = os.getcwd()
            if self.queryProcessor is None:
                dbStorage = DbStorage(CWD, dict['database'], int(dict['batchsize']))
                queryProcessor = QueryProcessor()
                queryProcessor.setDbStorage(dbStorage)
                self.queryProcessor = queryProcessor
            else:
                self.queryProcessor.getDbStorage().setDB(CWD, dict['database'], int(dict['batchsize']))
            print(f"Selected {dict['database']} database")

    
    def isValid(self, dict, keywords):
        i = 0
        for key in dict.keys():
            while(i<len(keywords) and key != keywords[i]):
                i+=1
            if i==len(keywords): return False
        return True
    
    def parse(self, query):
        self.process(self.tokenize(query))

    def DbPrompt(self):
        print("Please define or select a database to work with.")


def main():
    
    myparser = QueryParser()
    input_str = ""
    while True:
        command = input("fsDB> ")
        if len(command) == 0: continue
        if command == 'exit':
            break
        elif command[-1] == ';':
            input_str = " ".join([input_str,command[:-1]])
            try:
                myparser.parse(input_str)
            except:
                print("Invalid input. Quitting!")
                return
            input_str = ""
        else:
            input_str = " ".join([input_str,command])
    
    print("Bye")
    

if __name__ == "__main__":
    main()