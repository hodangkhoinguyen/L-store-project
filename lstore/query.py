from lstore.table import Table, Record, PageRange, RID, INDIRECTION_COLUMN, RID_COLUMN, TIMESTAMP_COLUMN, SCHEMA_ENCODING_COLUMN
from lstore.index import Index
from lstore.page import Page
import time

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
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        pass
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
        schema_encoding = '0' * self.table.num_columns
        key_column = self.table.key
        page_range_size = len(self.table.page_range_list)
        index = 4
        if (page_range_size == 0 \
            or (not self.table.page_range_list[page_range_size - 1].has_capacity)):
            new_page_range = PageRange()
            indirection_col = [None]
            rid_col = [RID(page_range_size, 0, 0)]
            timestamp_col = [int(time.time())]
            schema_encoding_col = [schema_encoding]
            base_page = [indirection_col, rid_col, timestamp_col, schema_encoding_col]
            
            for i in columns:
                page = Page()
                page.write(i)
                base_page.append(page)
            new_page_range.base_page_list.append(base_page)
            self.table.page_range_list.append(new_page_range)
        elif (self.table.page_range_list[page_range_size-1].base_page_list[self.table.page_range_list[page_range_size-1].getNumBase()-1][4].has_capacity()):
            base_page = self.table.page_range_list[page_range_size-1].base_page_list[self.table.page_range_list[page_range_size-1].getNumBase()-1]
            num_records = base_page[index].num_records
            base_page[INDIRECTION_COLUMN].append(None)
            base_page[RID_COLUMN].append(RID(page_range_size-1, self.table.page_range_list[page_range_size-1].getNumBase()-1, base_page[index].num_records-1))
            base_page[TIMESTAMP_COLUMN].append(int(time.time()) )
            base_page[SCHEMA_ENCODING_COLUMN].append(schema_encoding)
            
            for i in columns:
                base_page[index].write(i)  
                index += 1
        else:
            indirection_col = [None]
            rid_col = [RID(page_range_size-1, 0, 0)]
            timestamp_col = [int(time.time())]
            schema_encoding_col = [schema_encoding]
            base_page = [indirection_col, rid_col, timestamp_col, schema_encoding_col]
            for i in columns:
                page = Page()
                page.write(i)
                base_page.append(page)
            self.table.page_range_list[page_range_size-1].base_page_list.append(base_page)

    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, index_value, index_column, query_columns):
        pass
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass

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
