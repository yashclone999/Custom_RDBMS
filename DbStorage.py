# inport K and delimiter size
from operator import index
import os
import sys
import json
import pandas as pd
import numpy as np
import shutil
from pandas.api.types import is_float_dtype, is_int64_dtype
from memory_profiler import profile
import psutil

VAL_SEP = 1
ENTITY_SEP = 1
K = 3
MAX_UID = 999999999


# flag isFull - true/false
flagSize = 4

def join(a,b):
    return os.path.join(a,b)

def memory_used():
    process = psutil.Process(os.getpid())
    mem = process.memory_info()[0]/float(2**10) # in KB
    print(f"Memory Used: {mem} KB")

class DbStorage:

    dir = ""
    DB = ""
    tablePath = None
    DbPath = None

    def __init__(self, dir, DB, BATCH_SIZE) -> None:
        self.tableFilesSize = {}
        self.setDB(dir, DB, BATCH_SIZE)
        self.tableMetadata = None

    def setDB(self, dir, DB, BATCH_SIZE):
        self.dir = dir
        self.DB = DB
        self.createCurrentDbPath()
        self.createBufferPath()
        self.createConstraints()
        global K 
        K = BATCH_SIZE

    def createCurrentDbPath(self):
        self.DbPath = self.createDir(self.dir,self.DB)

    def createConstraints(self):
        path = join(self.DbPath,'constraints.json')
        if not os.path.isfile(path):
            with open(path, "w") as out:
                json.dump({},out)

    def writeToConstraints(self, obj):
        path = join(self.DbPath,'constraints.json')
        with open(path, "w") as out:
                json.dump(obj,out)
            
    
    def getConstraints(self):
        path = join(self.DbPath,'constraints.json')
        with open(path, 'r') as openfile:
                return json.load(openfile)
            
    def createBufferPath(self):
        self.bufferPath = self.createDir(self.DbPath,'buffer')

    def createDir(self,parent,child):
        path = join(parent,child)
        if not os.path.isdir(path):
            os.mkdir(path)
        return path

    def getCurrentDbPath(self):
        return self.DbPath

    def getCurrentTablePath(self,table):
        path = join(self.DbPath,table)
        if not os.path.isdir(path):
            os.mkdir(path)
        return path

    def createTable(self, table, cols, indexes):

        # TODO check if table exists
        path = join(self.DbPath,table)
        if os.path.isdir(path):
            print(f"{table} Exists")
            return
        
        path = self.getCurrentTablePath(table)
        tableFilesSize = self.calculateFileSizesForTable(table, cols, indexes, path) 
        self.storeMetaData(table,cols,tableFilesSize,path,indexes)

    def listFiles(self, dir):
        '''  IO to list num of files in directory '''
        return os.listdir(dir)

    def loadMetadata(self, tableName):
        path = self.getCurrentTablePath(tableName)
        if self.tableMetadata is None or self.tableMetadata['table'] != tableName:
            with open(join(path,'metadata.json'), 'r') as openfile:
                json_object = json.load(openfile)
            self.tableMetadata = json_object
        return self.tableMetadata

    def storeMetaData(self, tableName, cols, sizes, path, indexes):
        ''' inside /table/ dump metadata as json '''
        
        metadata = {}
        metadata['table'] = tableName
        metadata['columns'] = cols
        metadata['sizes'] = sizes
        metadata['uid'] = 1
        # metadata['indexes'] = indexes
        with open(join(path,'metadata.json'), "w") as out:
            json.dump(metadata,out)

    def calculateFileSizesForTable(self, table, cols, indexes, path):
        ''' calculate file sizes '''
        tableFileSize = {}

        rowSize = 0
        for col in cols:
            rowSize += cols[col]['size']
            rowSize += VAL_SEP
        
        tableFileSize['records'] =  flagSize + K*(rowSize)
        tableFileSize['row_size'] = rowSize

        ''' setup directories '''
        # IO create /records/

        path1 = join(path,'records')
        if not os.path.isdir(path1):
            os.mkdir(path1)

        # # IO create /indexes/
        # path2 = join(path,'indexes')
        # if not os.path.isdir(path2):
        #     os.mkdir(path2)
        
        # for index in indexes:
        #     # IO create /indexes/index/
        #     path = join(path2,index)
        #     if not os.path.isdir(path):
        #         os.mkdir(path)
        #     with open(path+f"/metadata.json", 'w') as out:
        #         json.dump({},out)
                
        return tableFileSize

    def insert(self,table,inputData,metadata):
        
        '''set path'''
        path = self.getCurrentTablePath(table)

        '''write records under /records/'''
        self.insertBatchRecords(inputData,path,table,metadata)

        # '''insert index data under /indexes/'''
        # path_indexes = join(path,'indexes')
        # for col in metadata['indexes']:
        #     if col == 'pk': continue
        #     self.insertIndexBatchRecords(inputData[['pk',col]],path_indexes,col,table,metadata)
        
        '''next uid is metadata['uid']''' 
        with open(join(path,'metadata.json'), "w") as out:
            json.dump(metadata,out) 

    def insertBatchRecords(self,inputData,path,table,metadata):
        path_table = join(path,'records')
        pk_pointer_dict = {}
        free_pointer_list = []
        record_file = None
        file_num = -1

        for i in range(len(inputData)):

            '''check if any file is open to write'''
            if len(free_pointer_list) == 0:

                if file_num != -1:

                    '''flush'''
                    with open(join(path_table,f"header_{file_num}.json"), "w") as out:
                        json.dump({'pks':pk_pointer_dict,'pointers':free_pointer_list},out) 
                    
                    record_file.seek(0)
                    record_file.write(bytes("true",'utf-8'))

                    file_num = -1
                    record_file.close()
                    record_file = None

                '''get new file'''
                file_num = self.getUnfilledRecordBatchFile(table,path_table,metadata)
                
                with open(join(path_table,f"header_{file_num}.json"), 'r') as openfile:
                    json_object = json.load(openfile)
                    pk_pointer_dict = json_object['pks']
                    free_pointer_list = json_object['pointers']

                record_file = open(join(path_table,f"batch_{file_num}"),'r+b')
            
            '''write into batch'''
            p = free_pointer_list.pop()
            pk_pointer_dict[inputData.iloc[i]['pk']] = p
            outBytes = self.createRowByte(inputData.iloc[i].array,metadata)
            record_file.seek((metadata['sizes']['row_size']*p)+flagSize,0)
            record_file.write(outBytes)

        '''flush'''
        with open((join(path_table,f"header_{file_num}.json")), "w") as out:
            json.dump({'pks':pk_pointer_dict,'pointers':free_pointer_list},out) 
        
        record_file.seek(0)
        if len(free_pointer_list) == 0: record_file.write(bytes("true",'utf-8'))
        else: record_file.write(bytes("fals",'utf-8'))

        file_num = -1
        record_file.close()
        record_file = None

    def createRowByte(self, input, metadata):
        s = ""
        for val in input:
            if val is not None:
                s += val 
            s += "|"
        s += ((metadata['sizes']['row_size'] - len(s))*'\0')
        return bytes(s,'utf-8')
        
    def getUnfilledRecordBatchFile(self, table, path, metadata):
        '''
        path = /table/records
        we need to look at each file flag header, if its not full select that file
        if no batch_ file - create a batch_0 - flag false header_0 - with empty pks, and full pointers and return 0
        if all batch_ file are full create new batch and header and return max val
        '''
        files = os.listdir(path)
        num_files = 0
        for file in files:
            if len(file) >= 5 and file[:5]=='batch':
                num_files += 1
                with open(join(path,file),'rb') as f:
                    f.seek(0)
                    if( str(f.read(4),'UTF-8') == 'fals' ): return file[6:]
        
        '''create header_num_files populate free pointers etc and batch_num_files populate flag'''
        with open(join(path,f"batch_{num_files}"),'wb') as f:
            f.seek(metadata['sizes']['records']-1)
            f.write(b'\0')
            f.seek(0)
            f.write(b'fals')
        
        free_pointer_list = [i for i in range(K)]
        with open(join(path,f"header_{num_files}.json"), "w") as out:
            json.dump({'pks': {},'pointers':free_pointer_list},out)
            
        return num_files

    def insertIndexBatchRecords(self, inputData, path, col, table, metadata):
        path = join(path,col)
        index_pk_list = None
        file_num = -1
        
        indexMetadata = None
        with open(join(path,f"metadata.json"), 'r') as openfile:
            indexMetadata = json.load(openfile)

        for i in range(len(inputData)):

            if file_num == -1 or indexMetadata[file_num] == K:
                
                if file_num != -1:
                    
                    '''flush'''
                    with open(join(path,f"batch_{file_num}.json"), "w") as out:
                        json.dump(index_pk_list,out)
            
                '''get new file'''
                file_num = self.getUnfilledIndexBatchFile(path,indexMetadata)

                with open(join(path,f"batch_{file_num}.json"), 'r') as openfile:
                    index_pk_list = json.load(openfile)


            '''write into index batch file'''
            if inputData.iloc[i][col] not in index_pk_list: index_pk_list[inputData.iloc[i][col]] = []
            if len(str(inputData.iloc[i][col])) > 0: index_pk_list[inputData.iloc[i][col]].append(inputData.iloc[i]['pk'])
            indexMetadata[file_num] += 1
        
        '''flush'''
        with open(join(path,f"batch_{file_num}.json"), "w") as out:
            json.dump(index_pk_list,out) 
        with open(join(path,f"metadata.json"), "w") as out:
            json.dump(indexMetadata,out) 

    def getUnfilledIndexBatchFile(self,path,indexMetadata):
        '''
        path = /table/indexes/index
        we need to look at each file flag header, if its not full select that file
        if no batch_ file - create a batch_0 - flag false header_0 - with empty pks, and full pointers and return 0
        if all batch_ file are full create new batch and header and return max val
        '''
        num_files = 0
        
        if len(indexMetadata) > 0:
            for k,v in indexMetadata.items():
                if v < K: return k
            num_files = len(indexMetadata.keys())
        
        with open(join(path,f"batch_{num_files}.json"), "w") as out:
                indexMetadata[num_files] = 0
                json.dump({},out)

        return num_files
        
    ''' no type conversion needed'''
    def searchCol(self, tableName, colName, operator, value, fetchData):
        '''value better be string data type
        for now single thread
        iterate over index/col/batch_i'''
        
        results = set()
        path = join(join(self.getCurrentTablePath(tableName),'indexes'),colName)
        files = os.listdir(path)
        for file in files:
            with open(join(path,file), 'r') as openfile:
                    data = json.load(openfile)

                    for k,v in data.items():
                        if operator is not None and not self.resolve(operator, k, value):
                            continue
                            
                        if fetchData == True:
                            results.update(v) 
                        else:
                            results.add(k) 
        return results
    
    def resolve(self, operator, arg1, arg2):

        if arg1 is None or (isinstance(arg1,str) and len(arg1) == 0):
            return operator == '==' and arg2 == "NULL"
        
        arg1 = self.to_float(arg1)
        arg2 = self.to_float(arg2)
        # TODO check if arg is an int/float or string (python must have inbuilt method), if yes then cast to int/float
        if operator == '<':
            return arg1 < arg2
        elif operator == '>':
            return arg1 > arg2
        elif operator == '<=':
            return arg1 <= arg2
        elif operator == '>=':
            return arg1 >= arg2
        elif operator == '==':
            return arg1 == arg2
        elif operator == '!=':
            return arg1 != arg2

    def searchOnPk(self, tableName, keys: set, fetchData):
        if keys is not None and fetchData == False:
            return keys
            
        metadata = self.loadMetadata(tableName) # dont open again and again
        # for now single thread
        # iterate over records/
        
        results = set()
        datarows = []
        path = join(self.getCurrentTablePath(tableName),'records')
        files = os.listdir(path)

        for file in files:
            if file[-5:]=='.json':
                    with open(join(path,file), 'r') as openfile:

                        # corresponding batch file
                        fileNum = file.split('.')[0].split('_')[1]
                        batchFile = open(join(path,f"batch_{fileNum}"),'rb')

                        data = json.load(openfile)
                        for k,p in data['pks'].items():
                            if (keys is not None and len(keys) > 0 and k not in keys):
                                continue
                            else:
                                if fetchData == True:
                                    datarows.append(self.seekRow(batchFile,metadata,p))
                                else:
                                    results.add(k) 
        
        return results if fetchData == False else datarows
    
    def updateOnPk(self, tableName, keys: set, updateData):
        '''
        keys - {k1,k2,...}
        updateData - {
                "table_name":name,
                table_name.col1:val1,
                table_name.col2:val2 
        }
        '''
        metadata = self.loadMetadata(tableName) # dont open again and again
        # for now single thread
        # iterate over records/

        path = join(self.getCurrentTablePath(tableName),'records')
        files = os.listdir(path)
        
        for file in files:
            if file[-5:]=='.json':
                with open(join(path,file), 'r') as headerFile:
                    data = json.load(headerFile)

                    # corresponding batch file
                    fileNum = file.split('.')[0].split('_')[1]
                    with open(join(path,f"batch_{fileNum}"),'r+b') as batchFile:
                        for k,p in data['pks'].items():
                            if int(k) in keys:
                                
                                '''read from batchfile'''
                                updated_row = self.getUpdatedRow(batchFile,metadata,p,updateData,tableName)

                                '''overwirte into batch'''
                                outBytes = self.createRowByte(updated_row,metadata)
                                
                                '''overwirte = erase+write'''
                                '''erase'''
                                batchFile.seek((metadata['sizes']['row_size']*p)+flagSize,0)
                                batchFile.write(b'\0'*metadata['sizes']['row_size'])
                                '''write'''
                                batchFile.seek((metadata['sizes']['row_size']*p)+flagSize,0)
                                batchFile.write(outBytes)

                          
    def deleteOnPk(self, tableName, keys: set):
        if len(keys) == 0: return
        
        '''
        keys - {k1,k2,...}
        '''
        metadata = self.loadMetadata(tableName) # dont open again and again
        # for now single thread
        # iterate over records/
        
        path = join(self.getCurrentTablePath(tableName),'records')
        files = os.listdir(path)
        is_modified = False

        for file in files:
            if file[-5:]=='.json':

                with open(join(path,file), 'r') as headerFile:
                    data = json.load(headerFile)

                    # corresponding batch file
                    fileNum = file.split('.')[0].split('_')[1]
                    
                    with open(join(path,f"batch_{fileNum}"),'r+b') as batchFile:
                        
                        for k,p in data['pks'].items():
                            if int(k) in keys:
                                
                                '''erase'''
                                batchFile.seek((metadata['sizes']['row_size']*p)+flagSize,0)
                                batchFile.write(b'\0'*metadata['sizes']['row_size'])
                                
                                '''make offset available now'''
                                data['pointers'].append(p)
                                
                                is_modified = True

                        if len(data['pointers'])>0:
                            batchFile.seek(0)
                            batchFile.write(bytes("fals",'utf-8'))

                    for k in keys:
                        if str(k) in data['pks']:
                            data['pks'].pop(str(k))

                if is_modified:
                    with open(join(path,file), "w") as headerFile:
                        json.dump(data,headerFile) 
                    is_modified = False
                        
                    
    def getUpdatedRow(self, batchFile, metadata, p, updateData, table):
        row = self.seekRow(batchFile,metadata,p)
        
        i = 0
        for col,v in metadata['columns'].items():
            col = (table+"."+col)
            if col in updateData:
                row[i] = str(updateData[col])
            i+=1
            
        return row
        
        
    def processBytes(self, row):
         # convert to array so that a dataframe can be created
        row = str(row,'UTF-8') 
        values = row.split('|')
        if len(values[-1]) == 0 or values[-1][0] == '\0':
            values = values[:-1]
        
        for i in range(len(values)):
            # handle none/empty value - shouldn't be processed
            if values[i] is None or len(values[i]) == 0:
                return None

        return values

    def processFilter(self, obj, df):
        result = set()
        if isinstance(obj[2], set):
            if(obj[1] == 'OR'):
                for i in range(len(df)):
                    if i in obj[0] or i in obj[2]:
                        result.add(i)

            elif(obj[1] == 'AND'):
                for i in range(len(df)):
                    if i in obj[0] and i in obj[2]:
                        result.add(i)

        else: 
            for i in range(len(df)):

                if self.resolve(obj[1], df.iloc[i][obj[2]], obj[0]):
                    result.add(i)
        
        return result

    def seekRow(self, batchFile, metadata, p):
        batchFile.seek((metadata['sizes']['row_size']*p)+flagSize,0)
        row = batchFile.read(metadata['sizes']['row_size'])
        return self.processBytes(row)
    
    def loadFile(self, dir, batchNum, metadata, table) -> pd.DataFrame:
        # for this batchNUm under dir load its header file and iterate over pks to get location
        # of rows then load the datarows[pk] = row

        # returns dataframe
        

        result = []
        with open(join(dir,f"header_{batchNum}.json"), 'r') as openfile:
            batchFile = open(join(dir,f"batch_{batchNum}"),'rb')
            data = json.load(openfile)

            if len(data['pks']) == 0: return None

            for k,p in data['pks'].items():
                row = self.seekRow(batchFile,metadata,p)
                if row is not None: result.append(row)
            batchFile.close()
            del data
        
            df = pd.DataFrame(np.array(result), columns=[table+"."+col for col in metadata['columns'].keys()])
            type_conversion = {}
            for k,v in metadata['columns'].items():
                if v['type'] == 'int':
                    type_conversion[table+"."+k] ='int64'
                elif v['type'] == 'float':
                    type_conversion[table+"."+k] = 'float64'
                elif v['type'] == 'date':
                    type_conversion[table+"."+k] = 'datetime64'
                else:
                    type_conversion[table+"."+k] = 'object'
            return df.astype(type_conversion)
        

    def join(self, adjList, stack, filterExpression, categorizeExpression, projectExpression, sortExpression, update=False, updateData=None, delete=False, deleteData=None):
        self.print_head = True
        tables = []
        dir = self.DbPath
        while len(stack)>0: 
            t = stack[-1]
            tables.append([t,[file[6:] for file in self.listFiles(join(join(dir,stack.pop()),"records")) if file[:5] == "batch"]])
        
        tables_joined_yet = set()
        self.nestedJoin(adjList, 0, tables, None, filterExpression, categorizeExpression, projectExpression, sortExpression, dir, tables_joined_yet, update, updateData, delete, deleteData)
        if categorizeExpression is not None: self.groupBy(categorizeExpression, projectExpression, sortExpression)
        self.externalSort(sortExpression)
        
        '''clear the files in buffer'''
        if categorizeExpression is not None or sortExpression is not None: self.clearDirectory(self.bufferPath)

        del stack
        del adjList
    
    def assignDtype(self, obj, col, data_types):
            if isinstance(obj,str):
                data_types[col] = 'object'
            elif isinstance(obj,int):
                data_types[col] = 'int64'
            elif isinstance(obj,float):
                data_types[col] = 'float64'

    def groupBy(self, categorizeExpression, projectExpression, sortExpression):
        '''
        creates batches(sorted) out of categorized directory
        '''
        # check if there si something to sort
        path = join(self.bufferPath,'categorize')
        if not os.path.isdir(path):
            return
        files =  self.listFiles(path)
        
        data_types = {}
        headers = []
        if categorizeExpression != "NULL": headers = [col for col in categorizeExpression]
        headers += [f"{val[0]}({val[1]})" for val in projectExpression]

        rows = []
        self.createDir(self.bufferPath,'sort')
        writePath = join(self.bufferPath,'sort')
        
        batchNum = 1

        for file in files:
            fpath = join(path,file)
            with open(fpath, 'r') as openfile:
                row = []
                obj = json.load(openfile)

                if categorizeExpression != "NULL":
                    for col in categorizeExpression:
                        row.append(obj['columns'][col])
                        self.assignDtype(obj['columns'][col],col,data_types)

                for val in projectExpression:
                    if val[0] == "COUNT":
                        row.append(obj['count'][val[1]])
                        data_types[f"{val[0]}({val[1]})"] = 'int64'
                    elif val[0] == "SUM":
                        row.append(obj['sum'][val[1]])
                        data_types[f"{val[0]}({val[1]})"] = 'float64'
                    elif val[0] == "AVG":
                        row.append(obj['sum'][val[1]]/obj['count'][val[1]])
                        data_types[f"{val[0]}({val[1]})"] = 'float64'
                    elif val[0] == "MAX":
                        row.append(obj['max'][val[1]])
                        self.assignDtype(obj['max'][val[1]],f"{val[0]}({val[1]})",data_types)
                    elif val[0] == "MIN":
                        row.append(obj['min'][val[1]])
                        self.assignDtype(obj['min'][val[1]],f"{val[0]}({val[1]})",data_types)

                rows.append(row)
                openfile.close()
            
            ''' flush '''
            if len(rows) == K:
                batch = pd.DataFrame(np.array(rows), columns=headers).astype(data_types) # define headers according to aggregate expression
                if sortExpression is not None: 
                    self.sort(batch, sortExpression, batchNum, writePath)
                    batchNum += 1
                else:
                    self.print_result(batch)
                rows = []
            os.remove(fpath)
        
        ''' flush '''
        if len(rows) > 0:
            batch = pd.DataFrame(np.array(rows), columns=headers).astype(data_types) # define headers according to aggregate expression
            if sortExpression is not None: 
                    self.sort(batch, sortExpression, batchNum, writePath)
                    batchNum += 1
            else:
                self.print_result(batch)
            del rows
            
            
    def print_result(self, batch):
        # print(batch.to_string(header=self.print_head,index=False))
        print("\n")
        print(batch.to_string(header=True,index=False))
        self.print_head = False
        

    def externalSort(self, sortExpression):
        if sortExpression is not None:
            sort_path = join(self.bufferPath,'sort')
            if not os.path.isdir(sort_path):
                return
            
            # ''' create final output to a file '''
            # outpath = join(sort_path,f'output.csv')
            # wrote_header = False

            
            outRows = []
            headerCols = None

            '''merge sort the stored files and print results line by line'''
            while True:
                rowToWrite = None
                fileToUpdate = None
                for file in self.listFiles(sort_path):
                    # if file == 'output.csv': continue

                    fpath = join(sort_path,file)
                    if os.path.getsize(fpath) == 0: 
                        continue 
                    
                    df = pd.read_csv(fpath)
                    if self.compareRows(df.iloc[0],rowToWrite,sortExpression) == True:
                        rowToWrite = df.iloc[0]
                        fileToUpdate = file
                
                if headerCols is None:
                    headerCols = [str(v) for v in rowToWrite.keys()]
            
                if rowToWrite is None:
                    break
                else: 
                    # if not wrote_header:
                    #     output = open(outpath, "w")
                    #     output.write("|".join([str(v) for v in rowToWrite.keys()]))
                    #     output.write("\n")
                    #     output.close()
                    #     wrote_header = True
                    
                    fpath = join(sort_path,fileToUpdate)
                    df = pd.read_csv(fpath)
                    if len(df)> 1: df.tail(-1).to_csv(fpath,index=False)
                    else: os.remove(fpath)

                    # output = open(outpath, "a")
                    # output.write("|".join([str(v) for v in rowToWrite.values]))
                    # output.write("\n")
                    # output.close()

                    outRows.append([v for v in rowToWrite.values])

                    ''' print output if outRows is of len batch_size'''
                    if len(outRows) == K:
                        outDf = pd.DataFrame(outRows, columns=headerCols) 
                        self.print_result(outDf)
                        outRows = []

                    # ''' print line by line '''
                    # if self.print_head:
                    #     print([str(v) for v in rowToWrite.keys()])
                    #     self.print_head = False
                    # print([v for v in rowToWrite.values])
                    
            
            ''' print output if outRows has content'''
            if len(outRows) > 0:
                outDf = pd.DataFrame(outRows, columns=headerCols) 
                self.print_result(outDf)
            
            del outRows
            del headerCols
            del rowToWrite        

            # ''' print output from file'''
            # print(pd.read_csv(outpath, delimiter='|').to_string())

    # @profile
    def nestedJoin(self, adjList, curr_table_pos, tables, base_batch: pd.DataFrame, \
                   filter_expression, categorizeExpression, projectExpression, sortExpression, Db_dir, tables_joined_yet, \
                    update=False, updateData=None, delete=False, deleteData=None):

        if curr_table_pos == len(tables): 
            self.baseCase(base_batch, filter_expression, update, updateData, delete, deleteData, 
                 categorizeExpression, projectExpression, sortExpression)
            return
        
        table = tables[curr_table_pos][0]
        batches = tables[curr_table_pos][1]
        metadata = self.loadMetadata(table)
        
        for batchNum in batches:
            loaded_batch = self.loadFile(join(Db_dir,f"{table}/records"),batchNum,metadata,table)
            if loaded_batch is None: continue
            
            joined_batch = self.merge(base_batch,loaded_batch,adjList,tables_joined_yet)
            del loaded_batch
            tables_joined_yet.add(table)
            if joined_batch is not None: 
                self.nestedJoin(adjList, curr_table_pos+1, tables, joined_batch, filter_expression, \
                                categorizeExpression, projectExpression, sortExpression, Db_dir, tables_joined_yet,\
                                      update, updateData, delete, deleteData)
            tables_joined_yet.remove(table)
            del joined_batch
    
    def checkValues(self, table, check_dict):
        batches = [file[6:] for file in self.listFiles(join(join(self.DbPath,table),"records")) if file[:5] == "batch"]
        metadata = self.loadMetadata(table)
        for batchNum in batches:
            loaded_batch = self.loadFile(join(self.DbPath,f"{table}/records"),batchNum,metadata,table)
            if loaded_batch is None: continue

            for i in range(len(loaded_batch)):

                row = loaded_batch.iloc[i]
                result = True
                for k,v in check_dict.items():
                    if str(row[k]) != str(v): 
                        result = False
                        break
                if result:
                    return result
            
        return False

    # @profile
    def baseCase(self, base_batch, filter_expression, update, updateData, delete, deleteData, 
                 categorizeExpression, projectExpression, sortExpression):
        
        # memory_used()

        ''' terminating conditions 
        
            either its
                filter 
                categorize (group by) 
                project (Count(col1)...) aggregations
                sort Count(col1) ASC
            or its 
                filter  
                project col1, col2
                sort colx
        '''
        if len(base_batch) > 0 and filter_expression is not None:
                self.filter(filter_expression,base_batch,update,updateData,delete,deleteData)
            
        if len(base_batch) > 0 and categorizeExpression is not None:
            self.categorize(base_batch, categorizeExpression, projectExpression)
            base_batch = None 
            
        else:
            if len(base_batch) > 0 and projectExpression is not None:
                if projectExpression != "*": self.project(projectExpression,base_batch)

            if len(base_batch) > 0 and sortExpression is not None:
                self.createDir(self.bufferPath,'sort')
                outpath = join(self.bufferPath,'sort')
                self.sort(base_batch, sortExpression, len(self.listFiles(outpath)), outpath)
                base_batch = None

        if base_batch is not None and len(base_batch) > 0 and not update and not delete: 
            self.print_result(base_batch)

    def merge(self, batch1: pd.DataFrame, batch2: pd.DataFrame, adjList, tables_joined_yet) -> pd.DataFrame:
        if batch1 is None:
            return batch2.copy()

        T2 = batch2.columns[0].split(".")[0]
        
        # find which table to join with
        for t in tables_joined_yet:
            if t in adjList[T2]:
                T1 = t
                break

        cols = adjList[T1][T2]
        T1_col = T1+"."+cols[0]
        T2_col = T2+"."+cols[1]

        merged = []
        for i in range(len(batch1)):
            for j in range(len(batch2)):
                if batch1.iloc[i][T1_col] == batch2.iloc[j][T2_col]:
                    row = []
                    row.extend(batch1.iloc[i])
                    row.extend(batch2.iloc[j])
                    merged.append(row)

        columns = []
        columns.extend(batch1.columns)
        columns.extend(batch2.columns)
        
        if len(merged)>0: return pd.DataFrame(merged, columns=columns)
        return None

    def filter(self, expression, df, update=False, updateData=None, delete=False, deleteData=None):
        tokens = [t.strip() for t in expression.split(" ")]

        real_tokens = []
        i = 0
        while i < len(tokens):
            if len(tokens[i]) > 0: 
                # handle string within quotes
                if tokens[i][0] == '"':
                    string = ""
                    tokens[i] = tokens[i][1:]
                    while tokens[i][-1] != '"':
                        string += (tokens[i]+" ")
                        i+=1
                    tokens[i] = tokens[i][:-1]  
                    string += (tokens[i])
                    real_tokens.append(string)
                else:
                    real_tokens.append(tokens[i])
            i+=1


        # stack only keeps the row numbers of the df(s), so its just bunch of numbers in memeory
        # for any df number of rows wont cross batch size
        stack = []
        for t in real_tokens:
            if len(t) == 0: continue
            if t != ')':
                stack.append(t)
            else:
                temp = []
                while(stack[-1] != '('):
                    temp.append(stack.pop())
                stack.pop()
                stack.append(self.processFilter(temp, df))

        res = stack.pop()

        for i in range(len(df)):
            if i not in res:
                df.drop(i, inplace=True)

        if update:
            update_keys = set()
            table = updateData['table_name']
            for i in range(len(df)):
                update_keys.add(int(df.iloc[i][table+'.pk']))
            if update_keys is not None and len(update_keys)>0: 
                self.updateOnPk(table,update_keys,updateData)
                # '''referential constraints'''
                self.referentialConstraints(table, updateData, df)
                
        
        if delete:
            delete_keys = set()
            table = deleteData['table_name']
            for i in range(len(df)):
                delete_keys.add(int(df.iloc[i][table+'.pk']))
            if delete_keys is not None and len(delete_keys)>0: 
                self.deleteOnPk(table,delete_keys)
                '''referential constraints'''
                globalConstraints = self.getConstraints()
                if table in globalConstraints and "delete" in globalConstraints[table]:
                    metadata = self.loadMetadata(table)
                    # check in global constraints if this table has constraint on udate
                    # if yes iterate over globalConstraints[table]['update'] -> obj:
                    for obj in globalConstraints[table]['delete']:
                        # here we need to perform delete on obj['to'] table
                        for i in range(len(df)):
                            filter_expression_vals = []
                            for ind in range(len(obj['from'])):
                                v = df.iloc[i][obj['from'][ind]]
                                if metadata['columns'][obj['from'][ind].split(".")[1]]["type"] == "str":
                                    filter_expression_vals.append(f"\"{v}\"")
                                else: filter_expression_vals.append(v)
                            filterExp = ""
                            for i in range(len(obj['to'])):
                                if len(filterExp) == 0:
                                    filterExp = f"( {obj['to'][i]} == {filter_expression_vals[i]} )"
                                else:
                                    filterExp = f"( {filterExp} AND ( {obj['to'][i]} == {filter_expression_vals[i]} ) )" 
                            toTable = obj['to'][0].split(".")[0]
                            if len(filterExp) > 0: 
                                print(f"Referential Integrity: Running @delete => {toTable} @filter => {filterExp};")
                                self.join({}, [toTable], filterExp, None, None, None, False, None, True,  {"table_name" : toTable})


    def referentialConstraints(self, table, updateData, df):
        globalConstraints = self.getConstraints()
        if table in globalConstraints and "update" in globalConstraints[table]:
            metadata = self.loadMetadata(table)
            # check in global constraints if this table has constraint on udate
            # if yes iterate over globalConstraints[table]['update'] -> obj:
            for obj in globalConstraints[table]['update']:

                # check if fields that we are updating affects other table or not
                proceed = False
                for i in range(len(obj['from'])):
                    if obj['from'][i] in updateData:
                        proceed = True
                        break
                
                if not proceed: continue

                
                # here we need to perform update on obj['to'] table
                col_indices = [i for i in range(len(obj['from']))]
                
                # the cols in col_indices must be updated for 'to' table, prepare query. Also we will need values from parent table in df
                for i in range(len(df)):
                    filter_expression_vals = []
                    for index in col_indices:
                        v = df.iloc[i][obj['from'][index]]
                        if metadata['columns'][obj['from'][index].split(".")[1]]["type"] == "str":
                            filter_expression_vals.append(f"\"{v}\"")
                        else: filter_expression_vals.append(v)

                    
                    toTable = obj['to'][0].split(".")[0]
                    filterExp = ""
                    updateExp = ""
                    updateDict = {"table_name" : toTable}
                    for j in range(len(col_indices)):
                        i = col_indices[j]
                        t = obj['to'][i].split(".")[1]
                        if obj['from'][i] in updateData:
                            updateDict[obj['to'][i]] = updateData[obj['from'][i]]
                            if len(updateExp) == 0:
                                updateExp = f"{t} = {updateData[obj['from'][i]]}"
                            else:
                                updateExp += f"| {t} = {updateData[obj['from'][i]]}"

                        if len(filterExp) == 0:
                            filterExp = f"( {obj['to'][i]} == {filter_expression_vals[j]} )"
                        else:
                            filterExp = f"( {filterExp} AND ( {obj['to'][i]} == {filter_expression_vals[j]} ) )" 
                    if len(filterExp) > 0: 
                        print(f"Referential Integrity: Running @update => {toTable} @values => {updateExp} @filter => {filterExp};")
                        self.join({}, [toTable], filterExp, None, None, None, True, updateDict, False, None)


    def project(self, projectExpression, base_batch: pd.DataFrame):
        keep = set(projectExpression)
        shed = [col for col in base_batch.columns if col not in keep]
        base_batch.drop(shed, axis=1, inplace=True)
        
    def sort(self, batch, sortExpression, batchNum, outpath):
        # sortExpression is list of columns to be sorted in this format: [[col, 'ASC'],[col2, 'DESC']]
        by = []
        bools = []
        for col in sortExpression:
            by.append(col[0])
            if col[1] == 'ASC':bools.append(True)
            else:bools.append(False)
        batch.sort_values(by=by, ascending=bools, inplace=True)
        batch.to_csv(join(outpath,f'batch_{batchNum}'),index=False)

    def compareRows(self, row1, row2, expression):
            
            # TODO check if row val is an int/float or string (python must have inbuilt method), if yes then cast to int/float
            if row2 is None:
                return True
            
            for exp in expression:
                col = exp[0]

                # # NULL case
                # if row1[col] is None or (isinstance(row1[col],str) and len(row1[col])==0):
                #     if exp[1] == 'ASC': return False
                #     else: return True

                # elif row2[col] is None or (isinstance(row2[col],str) and len(row2[col])==0):
                #     if exp[1] == 'DESC': return False
                #     else: return True
                # # NULL case

                a = self.to_float(row1[col])
                b = self.to_float(row2[col])
                if a == b: continue
                if exp[1] == 'ASC':
                    if a < b: return True
                    elif a > b: return False
                elif exp[1] == 'DESC':
                    if a > b: return True
                    elif a < b: return False

            return False
    
    def categorize(self, base_batch, categorizeExpression, projectExpression):
        '''
        group by colX colY aggregate Count(col1) , Sum(col2), Avg(col1)
        encode values of colX and colY in sequence
        using the encoded value, write row data in file - /buffer/categorize/encoded.json
        the file will have follwoing info - 
            for each col in [col1, col2]
                if col is int - sum of col so far, number of values in col so far
        thats all folks

        obj = 
        {
            "columns":{
                "colX": value,
                "colY": value
            },
            "sum":{
                "col1":sumSoFar,
                "col2":sumSoFar
            },
            "count":{
                "col1":countSoFar,
                "col2":countSoFar
            },
            "min":{
                "col1":minSoFar,
                "col2":minSoFar
            },
            "max":{
                "col1":maxSoFar,
                "col2":maxSoFar
            }
        }
        '''
        
        self.createDir(self.bufferPath,'categorize')
        group_by_cols = categorizeExpression
        aggregate_cols = set([c[1] for c in projectExpression])
        dtypes = base_batch.dtypes
        
        for i in range(len(base_batch)):
                encoded = ""
                obj = {"columns":{},"count":{},"sum":{},"min":{},"max":{}}
                if group_by_cols == "NULL":
                    encoded = "ALL"
                else:
                    for col in group_by_cols:
                        encoded = self.encode(encoded,str(base_batch.iloc[i][col]))
                        if dtypes[col] == 'object':
                            obj['columns'][col] = str(base_batch.iloc[i][col])
                        elif dtypes[col] == 'int64':
                            obj["columns"][col] = int(base_batch.iloc[i][col] )
                        elif dtypes[col] == 'float64':
                            obj["columns"][col] = float(base_batch.iloc[i][col] )
                    

                for col in aggregate_cols:
                    obj["count"][col] = 1 
                    if dtypes[col] == 'object':
                        obj["min"][col] = base_batch.iloc[i][col]
                        obj["max"][col] = base_batch.iloc[i][col]
                        continue
                    elif dtypes[col] == 'int64':
                        obj["min"][col] = int(base_batch.iloc[i][col])
                        obj["max"][col] = int(base_batch.iloc[i][col])
                        obj["sum"][col] = int(base_batch.iloc[i][col] )
                    elif dtypes[col] == 'float64':
                        obj["min"][col] = float(base_batch.iloc[i][col])
                        obj["max"][col] = float(base_batch.iloc[i][col])
                        obj["sum"][col] = float(base_batch.iloc[i][col] )

                path = join(join(self.bufferPath,'categorize'),f"{str(hash(encoded))}.json")

                if os.path.isfile(path):
                    with open(path, 'r') as openfile:
                        prev_obj = json.load(openfile)
                        for col in aggregate_cols:
                            if col in obj["sum"]:
                                obj["sum"][col] += prev_obj["sum"][col]
                            obj["count"][col] = prev_obj["count"][col]+1
                            obj["min"][col] = min(obj["min"][col],prev_obj["min"][col])
                            obj["max"][col] = max(obj["max"][col],prev_obj["max"][col])
                        openfile.close()

                with open(path, 'w') as openfile:
                        json.dump(obj,openfile)
                        openfile.close()
                

    def encode(self, encoded, val):
        postfix = "_".join([v.strip() for v in val.split(" ")])
        encoded  += ("--"+postfix)
        return encoded
    
    def clearDirectory(self, path):
        for filename in os.listdir(path):
            file_path = join(path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))

    def to_float(self, string):
        if string is not None and isinstance(string,str) and len(string)<20 and self.is_float(string): return float(string)
        return string

    def is_float(self,string):
        if string.replace(".", "").isnumeric():
            return True
        else:
            return False
        
        