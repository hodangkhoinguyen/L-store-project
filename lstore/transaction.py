from ast import Delete
from threading import Lock
from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query
from queue import LifoQueue

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.queryStack = LifoQueue()
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, table, args ))
        #if self.tableName == None:
            #self.tableName = table
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        #print("query ran")
        for query, table, args  in self.queries:

            originalVal = []
            queryName = ""
            page_directory = []

            
            if len(args) == 1:
                queryName = "delete"
            
            if len(args) == 2:
                queryName = "update"
                # This segment saves original values if it encounters an update operation and finds existing records
                saveOriginal = Query(table).currentValues(args[0])
                if not len(saveOriginal) == 0:
                    originalVal = saveOriginal

            if len(args) == 3:
                queryName = "read"
            
            # This segment gets original RID to be put into stack (not for insert)
            if not len(args) == 5:
                originalRID = Query(table).returnRID(args[0])
                if not originalRID == None:
                    page_directory = table.page_directory[originalRID[0]]

            # Runs the operation
            result = query(*args)

            # This segment gets original RID to be put into stack (for insert)
            if len(args) == 5:
                queryName = "insert"
                originalRID = Query(table).returnRID(args[0])
                if not originalRID == None:
                    page_directory = table.page_directory[originalRID[0]]
            
            # If op fails, it aborts
            if result == False:
                return self.abort()
            
            # Stack for aborting is appended if needed
            self.queryStack.put([queryName, args, originalRID, table, originalVal, page_directory])
            #print("stack appended")

        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        
        #print("aborting")
        
        #If stack of ops that went through is not empty, reverse the damage
        while self.queryStack.empty() == False:
            #print("in loop")
            (queryName, args, originalRID, table, originalVal, page_directory) = self.queryStack.get()
            
            lock = table.lock[originalRID[0]]
            if (type(lock) == Lock()):
                if(lock.locked()):
                    lock.release()
            elif lock.locked:
                lock.release() 
            # This skips select and aggregate ops
            if queryName == "read":
                continue

            # This undos insert with a delete
            if queryName == "insert":    
                Query(table).revert_insert(originalRID[0])
                continue

            # This undos update with an update of the original values
            if queryName == "update":    
                Query(table).revert_update(originalRID[0], originalVal)
                continue

            # This undos delete  (not done yet)
            if queryName == "delete":
                Query(table).revert_delete(originalRID[0], page_directory)
                continue


        return False

    def commit(self):
        while self.queryStack.empty() == False:
            #print("in loop")
            (queryName, args, originalRID, table, originalVal, page_directory) = self.queryStack.get()
            lock = table.lock[originalRID[0]]
            if (type(lock) == Lock()):
                if(lock.locked()):
                    lock.release()
            elif lock.locked:
                lock.release()        
            

        return True

