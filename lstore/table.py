import threading
from lstore.index import Index
from threading import Event, Thread, Timer
import time
import copy
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


            
class RepeatedTimer(object):
  def __init__(self, interval, function, *args, **kwargs):
    self._timer = None
    self.interval = interval
    self.function = function
    self.args = args
    self.kwargs = kwargs
    self.is_running = False
    self.next_call = time.time()
    self.start()

  def _run(self):
    self.is_running = False
    self.start()
    self.function(*self.args, **self.kwargs)

  def start(self):
    if not self.is_running:
      self.next_call += self.interval
      self._timer = threading.Timer(self.next_call - time.time(), self._run)
      self._timer.start()
      self.is_running = True

  def stop(self):
    self._timer.cancel()
    self.is_running = False
    
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
        self.dirty = False
        self.has_updated = None
        self.recent_tail = None
        
    def has_capacity(self):
        if (len(self.base_page_list) == self.base_limit and not self.base_page_list[self.base_limit - 1][4].has_capacity()):
            return False
        
        return True

    def getNumBase(self):
        return len(self.base_page_list)
    
    def getNumTail(self):
        return len(self.tail_page_list)
    
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
        self.counter_base = 1
        self.counter_tail = 1
        """
        page_director:
        key: rid = counter
        value: [page_range_number, base_page_number, slot_number]
        """
        self.db = None
        self.page_directory = {}        
        self.page_range_list = []
        self.lock = {}
        self.index = Index(self)
        
        # self.stopFlag = Event()
        # thread = MyThread(self.stopFlag, self)
        # thread.start()

        
    def create_lock(self):
        for key in self.page_directory:
            if (key[0] == 'b'):
                self.lock[key] = None
                
    def merge(self):
        print("merge is happening")
        for page_range in self.page_range_list:
            recent_tail = page_range.recent_tail
            new_updated = page_range.recent_tail
            
            #check if there's any recent updates that haven't been merged
            if page_range.has_updated != recent_tail: 
                merge_base_page_list = copy.deepcopy(page_range.base_page_list)           
                
                for i in range(len(merge_base_page_list)):
                    merge_base_page = merge_base_page_list[i]
                    
                    for j, rid in enumerate(merge_base_page[RID_COLUMN], 0):
                        merge_base_page[TIMESTAMP_COLUMN][j] = int(time.time())
                        rid_tail = merge_base_page[INDIRECTION_COLUMN][j]
                        
                        #check if this record has any update or ever merged
                        if (rid_tail != None and page_range.has_updated != None and int(rid_tail[1:]) <= int(page_range.recent_tail[1:])):
                            print('z')
                            if (rid != -1):                            
                                for k in range(self.table.num_columns):                                
                                    location_tail = self.page_directory[rid_tail]
                                    schema_encoding = merge_base_page[SCHEMA_ENCODING_COLUMN][j]
                                    if schema_encoding[k] == 1:
                                        merge_base_page[4+k][j].write(page_range.tail_page_list[location_tail[0]].tail_page_list[location_tail[1]][4+k].read(location_tail[2]))
                    
                    merge_base_page[INDIRECTION_COLUMN] = page_range.base_page_list[i][INDIRECTION_COLUMN].copy()
                    merge_base_page[SCHEMA_ENCODING_COLUMN] = page_range.base_page_list[i][SCHEMA_ENCODING_COLUMN].copy()
                page_range.base_page_list = merge_base_page_list 
                page_range.has_updated = new_updated
                self.db.write_page_range(page_range)
       
    # def timer(self):
    #     threading.Thread(self.__merge(), daemon=True)
        
    # def printtest(self):
    #     while (True):
    #         print("hello")
    #         time.sleep(4)
class MyThread(Thread):
    def __init__(self, event, table):
        Thread.__init__(self)
        self.stopped = event
        self.table : Table = table
    def run(self):
        while not self.stopped.wait(0.5):
            self.table.merge()