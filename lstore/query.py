from msilib import schema
from threading import Lock, RLock
import threading
from lstore.table import Table, Record, PageRange, INDIRECTION_COLUMN, RID_COLUMN, TIMESTAMP_COLUMN, SCHEMA_ENCODING_COLUMN
from lstore.index import Index
from lstore.page import Page
import time
class CustomRLock(threading._PyRLock):

    @property
    def locked(self):
        return bool(self._count)
class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table: Table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified primary_key
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        rid = self.table.index.indices[self.table.key].locate(primary_key)
        
        if rid == None:
            return False
        rid = rid[0]
        lock = self.table.lock[rid]
        if lock != None:
            if type(lock) == type(Lock()):
                if lock.locked():
                    try:
                        lock.release()
                    except:
                        return False
                    lock.acquire()
                else:
                    lock.acquire()
            else:
                if (lock.locked):
                    return False
                else:
                    self.table.lock[rid] = Lock()
                    lock = self.table.lock[rid]
                    lock.acquire()
        else:
            self.table.lock[rid] = Lock()
            lock = self.table.lock[rid]
            lock.acquire()
                
        location = self.table.page_directory[rid]
        index_in_bufferpool = self.table.db.use_bufferpool(self.table.page_range_list[location[0]])
        if (index_in_bufferpool == -1):
            return False
        
        self.table.db.dirty[index_in_bufferpool] = True
        
        
        """
        location: <- list of informations
        [0] page_range
        [1] base_page
        [2] slot        
        """
        self.table.page_range_list[location[0]].base_page_list[location[1]][RID_COLUMN][location[2]] = -1        
        self.table.page_directory.pop(rid)
        return True
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    # INDIRECTION_COLUMN = 0
    # RID_COLUMN = 1
    # TIMESTAMP_COLUMN = 2
    # SCHEMA_ENCODING_COLUMN = 3
    def insert(self, *columns):
        if not len(columns) == self.table.num_columns:
            return False
        key_column = self.table.key
        primary_key = columns[key_column]
        
        #check if there is any existing record
        if (self.table.index.locate(key_column, primary_key) != None):
            return False
        
        schema_encoding = '0' * self.table.num_columns
        
        page_range_size = len(self.table.page_range_list)
        index = 4

        rid = 'b' + str(self.table.counter_base)
        if (page_range_size == 0 \
            or (not self.table.page_range_list[page_range_size - 1].has_capacity())):
            new_page_range = PageRange()
            new_page_range.path = self.table.name + " page_range " + str(len(self.table.page_range_list) )            
            self.table.page_range_list.append(new_page_range)
            
            index_in_bufferpool = self.table.db.use_bufferpool(new_page_range)
            if (index_in_bufferpool == -1):
                return False
            
            self.table.db.dirty[index_in_bufferpool] = True
            
            indirection_col = [None]
            location = [page_range_size, 0, 0]
            rid_col = [rid]
            timestamp_col = [int(time.time())]
            schema_encoding_col = [schema_encoding]
            base_page = [indirection_col, rid_col, timestamp_col, schema_encoding_col]
            
            for i in columns:
                page = Page()
                page.write(i)
                base_page.append(page)
            new_page_range.base_page_list.append(base_page)
        elif (self.table.page_range_list[page_range_size-1].base_page_list[self.table.page_range_list[page_range_size-1].getNumBase()-1][4].has_capacity()):
            index_in_bufferpool = self.table.db.use_bufferpool(self.table.page_range_list[page_range_size-1])
            if (index_in_bufferpool == -1):
                return False
            
            self.table.db.dirty[index_in_bufferpool] = True
            
            base_page = self.table.page_range_list[page_range_size-1].base_page_list[self.table.page_range_list[page_range_size-1].getNumBase()-1]
            base_page[INDIRECTION_COLUMN].append(None)
            base_page[RID_COLUMN].append(rid)
            location = [page_range_size-1, self.table.page_range_list[page_range_size-1].getNumBase()-1, base_page[index].num_records]
            base_page[TIMESTAMP_COLUMN].append(int(time.time()) )
            base_page[SCHEMA_ENCODING_COLUMN].append(schema_encoding)
            
            for i in columns:
                base_page[index].write(i)  
                index += 1
        else:
            index_in_bufferpool = self.table.db.use_bufferpool(self.table.page_range_list[page_range_size-1])
            if (index_in_bufferpool == -1):
                return False
            
            self.table.db.dirty[index_in_bufferpool] = True
            indirection_col = [None]
            location = [page_range_size-1, self.table.page_range_list[page_range_size-1].getNumBase(), 0]
            rid_col = [rid]
            timestamp_col = [int(time.time())]
            schema_encoding_col = [schema_encoding]
            base_page = [indirection_col, rid_col, timestamp_col, schema_encoding_col]
            for i in columns:
                page = Page()
                page.write(i)
                base_page.append(page)
            self.table.page_range_list[page_range_size-1].base_page_list.append(base_page)
        
        
        self.table.index.insert(key_column, primary_key, rid)
        self.table.counter_base += 1
        self.table.page_directory[rid] = location
        self.table.lock[rid] = Lock()
        self.table.lock[rid].acquire()
        return True

    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def currentValues(self, primary_key):   
        rid_list = self.table.index.locate(self.table.key, primary_key)
        
        if (rid_list == None):
            return False
        
        result = []
        
        for rid in rid_list:
            if (rid in self.table.page_directory):
                location = self.table.page_directory[rid]
                base_page = self.table.page_range_list[location[0]].base_page_list[location[1]]
                indirection = base_page[INDIRECTION_COLUMN][location[2]]
                timestamp = base_page[TIMESTAMP_COLUMN][location[2]]
                schema_encoding = base_page[SCHEMA_ENCODING_COLUMN][location[2]]
            
                result = [indirection, rid, timestamp, schema_encoding]
                for i in range(self.table.num_columns):
                    result.append(base_page[i+4].read(location[2]))                   
                    
        return result

        # Only for use with the transaction class for now. Retrieves the rid of a record that is affected by transaction
    def returnRID(self, primary_key):
        rid = self.table.index.locate(self.table.key, primary_key)
        return rid
    
    def select(self, index_value, index_column, query_columns):
        rid_list = self.table.index.locate(index_column, index_value)
        
        if (rid_list == None):
            return False
        
        result = []
        
        for rid in rid_list:
            if (rid in self.table.page_directory):
                lock = self.table.lock[rid]
                if lock != None:
                    if type(lock) == type(Lock()):
                        try:
                            lock.release()
                        except:
                            return []
                        self.table.lock[rid] = CustomRLock()
                        lock = self.table.lock[rid]
                    else:
                        lock.acquire()
                else:
                    self.table.lock[rid] = CustomRLock()
                    lock = self.table.lock[rid]
                    lock.acquire()
                        
                location = self.table.page_directory[rid]
                index_in_bufferpool = self.table.db.use_bufferpool(self.table.page_range_list[location[0]])
                if (index_in_bufferpool == -1):
                    return []
                
                self.table.db.dirty[index_in_bufferpool] = True
                base_page = self.table.page_range_list[location[0]].base_page_list[location[1]]
                columns = []
                for i in range(self.table.num_columns):
                    schema_encoding = base_page[SCHEMA_ENCODING_COLUMN][location[2]]
                    no_update_schema = '0' * self.table.num_columns
                    
                    if (schema_encoding != no_update_schema):                        
                        rid_tail = base_page[INDIRECTION_COLUMN][location[2]]
                        location_tail = self.table.page_directory[rid_tail]
                        tail_page = self.table.page_range_list[location_tail[0]].tail_page_list[location_tail[1]]
                            
                    if (query_columns[i] == 1):
                        if schema_encoding[i] == '0':
                            columns.append(base_page[i+4].read(location[2]))
                        else:
                            columns.append(tail_page[i+4].read(location_tail[2]))
                    else:
                        columns.append(None)
                
                record = Record(rid, self.table.key, columns)
                result.append(record)
        return result
            
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        rid_list = self.table.index.indices[self.table.key].locate(primary_key)
        if rid_list == None or len(columns) != self.table.num_columns:
            return False
        
        rid = rid_list[0]
        
        lock = self.table.lock[rid]
        if lock != None:
            if type(lock) == type(Lock()):
                if lock.locked():
                    try:
                        lock.release()
                    except:
                        return False
                    lock.acquire()
                else:
                    lock.acquire()
            else:
                if (lock.locked):
                    return False
                else:
                    self.table.lock[rid] = Lock()
                    lock = self.table.lock[rid]
                    lock.acquire()
        else:
            self.table.lock[rid] = Lock()
            lock = self.table.lock[rid]
            lock.acquire()
            
        location = self.table.page_directory[rid]
        index_in_bufferpool = self.table.db.use_bufferpool(self.table.page_range_list[location[0]])
        if (index_in_bufferpool == -1):
            return False
        
        self.table.db.dirty[index_in_bufferpool] = True
        base_page = self.table.page_range_list[location[0]].base_page_list[location[1]]
        indirection = base_page[INDIRECTION_COLUMN][location[2]]
        schema_encoding = base_page[SCHEMA_ENCODING_COLUMN][location[2]]
        rid_tail = 't' + str(self.table.counter_tail)       
        base_page[INDIRECTION_COLUMN][location[2]] = rid_tail
        
        if (indirection == None):
            indirection = rid[0]
            
        current_page_range = self.table.page_range_list[location[0]]
        time_stamp = int(time.time())
        if (current_page_range.getNumTail() == 0 or not current_page_range.tail_page_list[current_page_range.getNumTail()-1][4].has_capacity()):
            location_tail = [location[0], current_page_range.getNumTail(), 0]
            current_page_range.tail_page_list.append([[],[],[],[]])
            tail_page = current_page_range.tail_page_list[current_page_range.getNumTail()-1]
            tail_page[RID_COLUMN].append(rid_tail)
            tail_page[INDIRECTION_COLUMN].append(indirection)
            tail_page[TIMESTAMP_COLUMN].append(time_stamp)
            schema = ""
            for i in range(self.table.num_columns):
                tail_page.append(Page())
                if (columns[i] != None):
                    schema += '1'
                    tail_page[i+4].write(columns[i])
                else:
                    schema += schema_encoding[i]
                    if indirection[0] == 'b':
                        tail_page[i+4].write(base_page[i+4].read(location[2]))
                    else:
                        location_previous_tail = self.table.page_directory[indirection]
                        tail_page[i+4].write(current_page_range.tail_page_list[location_previous_tail[1]][i+4].read(location_previous_tail[2]))  
            schema_encoding = schema          
            tail_page[SCHEMA_ENCODING_COLUMN].append(schema_encoding)
            
        else:
            location_tail = [location[0], current_page_range.getNumTail()-1, current_page_range.tail_page_list[current_page_range.getNumTail()-1][4].num_records]
            tail_page = current_page_range.tail_page_list[current_page_range.getNumTail()-1]
            tail_page[RID_COLUMN].append(rid_tail)
            tail_page[INDIRECTION_COLUMN].append(indirection)
            tail_page[TIMESTAMP_COLUMN].append(time_stamp)
            schema = ""
            for i in range(self.table.num_columns):
                tail_page.append(Page())
                if (columns[i] != None):
                    schema += '1'
                    tail_page[i+4].write(columns[i])
                else:
                    schema += schema_encoding[i]
                    if indirection[0] == 'b':
                        tail_page[i+4].write(base_page[i+4].read(location[2]))
                    else:
                        location_previous_tail = self.table.page_directory[indirection]
                        tail_page[i+4].write(current_page_range.tail_page_list[location_previous_tail[1]][i+4].read(location_previous_tail[2]))  
            schema_encoding = schema          
            tail_page[SCHEMA_ENCODING_COLUMN].append(schema_encoding)
        current_page_range.recent_tail = rid_tail
        self.table.page_directory[rid_tail] = location_tail        
        base_page[SCHEMA_ENCODING_COLUMN][location[2]] = schema_encoding        
        self.table.counter_tail += 1
        
        return True
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        rid_list = self.table.index.locate_range(start_range, end_range, self.table.key)
        if (rid_list == None):
            return False
        result = 0
        for i in rid_list:
            rid = i[0]
            lock = self.table.lock[rid]
            if lock != None:
                if type(lock) == type(Lock()):
                    try:
                        lock.release()
                    except:
                        return False
                    self.table.lock[rid] = CustomRLock()
                    lock = self.table.lock[rid]
                else:
                    lock.acquire()
            else:
                self.table.lock[rid] = CustomRLock()
                lock = self.table.lock[rid]
                lock.acquire()
            if (rid in self.table.page_directory):
                if type(lock) == type(Lock()):
                    try:
                        lock.release()
                    except:
                        return False
                    self.table.lock[rid] = CustomRLock()
                    lock = self.table.lock[rid]
                else:
                    lock.acquire()
                location = self.table.page_directory[rid]
                index_in_bufferpool = self.table.db.use_bufferpool(self.table.page_range_list[location[0]])
                if (index_in_bufferpool == -1):
                    return False
                
                self.table.db.dirty[index_in_bufferpool] = True
                base_page = self.table.page_range_list[location[0]].base_page_list[location[1]]
                if (base_page[SCHEMA_ENCODING_COLUMN][location[2]][aggregate_column_index] == '0'):
                    result += base_page[aggregate_column_index+4].read(location[2])
                else:
                    rid_tail = base_page[INDIRECTION_COLUMN][location[2]]
                    location_tail = self.table.page_directory[rid_tail]
                    tail_page = self.table.page_range_list[location_tail[0]].tail_page_list[location_tail[1]]
                    result += tail_page[aggregate_column_index+4].read(location_tail[2])
                    
        return result

    def revert_insert(self, rid):
        
        if rid == None or not rid in self.table.page_directory:
            return False
        
        location = self.table.page_directory[rid]
        index_in_bufferpool = self.table.db.use_bufferpool(self.table.page_range_list[location[0]])
        if (index_in_bufferpool == -1):
            return False
        
        self.table.db.dirty[index_in_bufferpool] = True
        
        
        """
        location: <- list of informations
        [0] page_range
        [1] base_page
        [2] slot        
        """
        self.table.page_range_list[location[0]].base_page_list[location[1]][RID_COLUMN][location[2]] = -1        
        self.table.page_directory.pop(rid)
    
    def revert_update(self, rid, *columns):
        if (rid == None or not rid in self.table.page_directory):
            return False
        
        location = self.table.page_directory[rid]
        index_in_bufferpool = self.table.db.use_bufferpool(self.table.page_range_list[location[0]])
        if (index_in_bufferpool == -1):
            return False
        
        self.table.db.dirty[index_in_bufferpool] = True
        
        base_page = self.table.page_range_list[location[0]].base_page_list[location[1]]
        base_page[INDIRECTION_COLUMN][location[2]] = columns[0]
        base_page[TIMESTAMP_COLUMN][location[2]] = columns[1]
        base_page[SCHEMA_ENCODING_COLUMN][location[2]] = columns[2]
            
        return True 
           
    def revert_delete(self, rid, location):
        if (rid == None or not rid in self.table.page_directory):
            return False
        
        location = self.table.page_directory[rid]
        index_in_bufferpool = self.table.db.use_bufferpool(self.table.page_range_list[location[0]])
        if (index_in_bufferpool == -1):
            return False
        
        self.table.db.dirty[index_in_bufferpool] = True
        
        base_page = self.table.page_range_list[location[0]].base_page_list[location[1]]
        base_page[RID_COLUMN][location[2]] = rid
        self.table.page_directory[rid] = [location[0], location[1], location[2]]
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

