import DbStorage
import pandas as pd
import numpy as np

class QueryProcessor:
    

    def __init__(self) -> None:
        self.dbStorageObj: DbStorage.DbStorage = None

    def setDbStorage(self, dbStorageObj):
        self.dbStorageObj = dbStorageObj
    
    def getDbStorage(self):
        return self.dbStorageObj

    def processConstraints(self, queryDict):
        # updates a global file called contraints for DB
        '''
        grab the constraints object from file - globalConstraints
        '''
        globalConstraints = self.dbStorageObj.getConstraints()
        constraints = [constraint.strip()[1:] for constraint in queryDict['constraint'].split("]")]
        for constraint in constraints:
            if len(constraint.strip()) == 0: continue
            temp = constraint.split("references")
            childCols = [c.strip() for c in temp[0].split(",")]
            parentCols = [c.strip() for c in temp[1].split(",")]
            childTable = childCols[0].split(".")[0]
            parentTable = parentCols[0].split(".")[0]
            if childTable not in globalConstraints:
                globalConstraints[childTable] = {"insert":[],"update":[],"delete":[]}
            if parentTable not in globalConstraints:
                globalConstraints[parentTable] = {"insert":[],"update":[],"delete":[]}

            globalConstraints[childTable]["insert"].append({'ops':'filter','from':childCols,'to':parentCols})
            globalConstraints[parentTable]["update"].append({'ops':'update','from':parentCols,'to':childCols})
            globalConstraints[parentTable]["delete"].append({'ops':'delete','from':parentCols,'to':childCols})

        '''write constraints object back to file '''
        self.dbStorageObj.writeToConstraints(globalConstraints)

    def DDL(self, queryDict):
        cols = self.processColumns(queryDict)
        indexes = None #self.processindexes(queryDict)
        table = queryDict['create']
        if 'constraint' in queryDict: self.processConstraints(queryDict)

        self.dbStorageObj.createTable(table,cols,indexes)
        print(f"Successfully created {table} table")

    def processColumns(self, queryDict):
        '''
        returns
        {
            colName:{
                "type":    ,
                "size":
            },
            colName:{
                "type":    ,
                "size":
            },
        }
        '''
        cols = queryDict['columns'].split(",")
        dict = {"pk":{"type":"int","size":9}}
        for col in cols:
            tokens = col.strip().split(" ")
            colName = tokens[0].strip()
            colType = tokens[1].strip()
            colSize = int(tokens[2].strip())
            dict[colName] = {"type":colType,"size":colSize}
        return dict

    ''' Not needed '''
    def processKeys(self, queryDict):
        '''
        returns
        set(key,key,..)
        '''
        keys = queryDict['keys'].split(",")
        keySet = set()
        for key in keys:
            keySet.add(key.strip())
        return keySet

    def processindexes(self, queryDict):
        '''
        returns
        set(index,index,..)
        '''
        res = []
        indexes = queryDict['indexes'].split(",")
        for index in indexes:
            res.append(index.strip())
        return res

    def DML(self, query):
        tables = query['use']

        sortExpression = query.get('sort')
        if sortExpression is not None: sortExpression = [[val.strip() for val in col.strip().split(' ') if len(val)>0] for col in sortExpression.split(',') if len(col)>0]

        categorizeExpression = None
        projectExpression = None
        if query.get('categorize') is not None: 
            if query.get('categorize').strip() == "NULL": categorizeExpression = "NULL"
            else: categorizeExpression = [c.strip() for c in query.get('categorize').split(",")]
        if categorizeExpression is not None: projectExpression = [[col.strip() for col in cols.strip().split(":")] for cols in query.get('project').split(",")]
        else:  
            if query.get('project').strip() == "*": projectExpression = "*"
            else: projectExpression = [col.strip() for col in query.get('project').split(",")]

        self.join(tables, query.get('combine'), query.get('filter'), categorizeExpression, projectExpression, sortExpression)
        
        del query
        if categorizeExpression: del categorizeExpression
        if projectExpression: del projectExpression
        if sortExpression: del sortExpression

    def insert(self, queryDict):
        # validate if table exists
        table = queryDict.get('populate')

        
        ''' 
        read from /table metadata
        required - next available uid, size of each col, order of columns 
        '''
        metadata = self.dbStorageObj.loadMetadata(table)
        uid = metadata['uid']


        '''
        check if there is constraint on table for insert
        '''
        checkConstraint = False
        globalConstraints = self.dbStorageObj.getConstraints()
        if table in globalConstraints and "insert" in globalConstraints[table]:
            checkConstraint = True


        ''' rows to insert in file'''
        rows = [] 
        
        '''list of column sizes'''
        sizes = []
        cols = {}
        ind = 0
        for k,v in metadata['columns'].items():
            sizes.append(v['size'])
            cols[k] = ind
            ind+=1
       

        for values in queryDict.get('values').split(']'):
            if len(values) == 0: break
            values = values.strip()[1:] # ignore '['
            
            # check if total len is in limit
            temp = []
            total_len = 0
            i = 1
            isValid = True
            for val in values.split('|'):
                v = val.strip()
                if len(v) > sizes[i]: 
                    isValid = False
                    break
                total_len += (len(v)+DbStorage.VAL_SEP)
                temp.append(v)
                i += 1

            if isValid == False: continue
            arr = [str(uid)] + temp
            if metadata['sizes']['row_size'] < total_len or len(metadata['columns']) != len(arr): continue
            
            canInsert = True
            if checkConstraint:
                # check 
                for obj in globalConstraints[table]['insert']:

                    filter_expression_vals = []
                    for val in obj['from']:
                        filter_expression_vals.append(arr[cols[val.split(".")[1]]])

                    check_dict = {}
                    toTable = obj['to'][0].split(".")[0]
                    for i in range(len(obj['to'])):
                        check_dict[obj['to'][i]] = filter_expression_vals[i] 

                    if not self.dbStorageObj.checkValues(toTable,check_dict):
                        canInsert = False
                        print(f"Invalid Input {arr}: References non-existing values in {toTable}")
                        break


            if canInsert:
                rows.append(arr)
                uid +=1
                
                

        if len(rows) > 0:
            columns = [colName for colName in metadata['columns'].keys()]
            data = pd.DataFrame(np.array(rows),columns=columns)
            metadata['uid'] = uid
            self.dbStorageObj.insert(table,data,metadata) 

        print(f"{len(rows)} rows inserted into {table} table")
        

    def join(self, tables, joinExpression, filterExpression, categorizeExpression, projectExpression, sortExpression, update=False, updateData=None, delete=False, deleteData=None):

        # break the expression to create adj list
        adjList = {}
        allTables = set()
        
        if joinExpression is not None:
            joins = [join for join in joinExpression.split(",")]
            for join in joins:
                tables = join.split("=")
                T1_col = tables[0].strip().split(".")
                T2_col = tables[1].strip().split(".")
                if T1_col[0] not in adjList:
                    adjList[T1_col[0]] = {}
                if T2_col[0] not in adjList:
                    adjList[T2_col[0]] = {}
                adjList[T1_col[0]][T2_col[0]] = [T1_col[1],T2_col[1]]
                adjList[T2_col[0]][T1_col[0]] = [T2_col[1],T1_col[1]]
                allTables.add(T1_col[0])
                allTables.add(T2_col[0])
        else:
            allTables.add(tables)

        stack = []
        visited = set()
        for table in allTables:
            self.dfs(adjList,stack,table,visited)

        del visited
        del allTables

        self.dbStorageObj.join(adjList,stack,filterExpression,categorizeExpression,projectExpression,sortExpression,update,updateData,delete,deleteData)



    def dfs(self, adjList, stack, curr, visited):
        if curr in visited:
            return
        
        visited.add(curr)

        if curr in adjList:
            for neigh in adjList.get(curr).keys():
                self.dfs(adjList,stack,neigh,visited)
        
        stack.append(curr)

    def delete(self, queryDict):
        # validate if table exists
        table = queryDict.get('delete')
        
        ''' data to delete'''
        deleteData = {"table_name":table}
     
        self.join(table, None, queryDict.get('filter'), None, None, None, update=False, updateData=None, delete=True, deleteData=deleteData)
        print(f"Delete operation completed on {table} table")

    def update(self, queryDict):
        # validate if table exists
        table = queryDict.get('update')


        ''' data to update'''
        updateData = {"table_name":table}
        
        for values in queryDict.get('values').split('|'):
            if len(values) == 0: continue
            temp = values.split('=')
            if temp[0].strip() == 'pk':
                print(f"Invalid Input: Updating Primary Key not allowed")
                return
            updateData[table+"."+temp[0].strip()] = temp[1].strip()
            
     
        self.join(table, None, queryDict.get('filter'), None, None, None, update=True, updateData=updateData)
        print(f"Update operation completed on {table} table")
        

        

        
