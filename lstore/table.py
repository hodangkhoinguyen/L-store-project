from lstore.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class PageRange:
    
    def __init__(self):
        self.path = None
        self.base_limit = 16
        self.base_page_list = []
        self.tail_page_list = []
        
    def has_capacity(self):
        if (self.num_base == self.base_limit and not self.base_page[self.num_base - 1].has_capacity):
            return False
        
        return True

    def getNumBase(self):
        return len(self.base_page_list)
    
    def getNumTail(self):
        return len(self.tail_page_list)
    
# class RID:
#     def __init__(self, page_range_number, page_number, slot_number):
#         self.page_range_number = page_range_number
#         self.page_number = page_number
#         self.slot_number = slot_number
        
#     def __eq__(self, other):
#         if (self.page_range_number == other.page_range_number and self.page_number == other.page_number and self.slot_number == other.slot_number):
#             return True
#         return False
    
#     def __lt__(self, other):
#         if self.page_range_number < other.page_range_number or (self.page_range_number == other.page_range_number and \
#             (self.page_number < other.page_number or (self.page_number == other.page_number and self.slot_number < other.slot_number))):
#             return True
#         return False
        
#     def __gt__(self, other):
#         if self.page_range_number > other.page_range_number or (self.page_range_number == other.page_range_number and \
#             (self.page_number > other.page_number or (self.page_number == other.page_number and self.slot_number > other.slot_number))):
#             return True
#         return False
    
class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.counter = 0
        
        """
        page_director:
        key: rid = counter
        value: [page_range_number, base_page_number, slot_number]
        """
        self.page_directory = {}        
        self.page_range_list = []
        self.index = Index(self)
        
        self.num_records = 0
        pass

    def __merge(self):
        print("merge is happening")
        pass
 
