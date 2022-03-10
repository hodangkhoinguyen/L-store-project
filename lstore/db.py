from email.mime import base
import json
import os
from lstore.table import Table, PageRange
from lstore.page import Page

def int_keys(ordered_pairs):
        result = {}
        for key, value in ordered_pairs:
            try:
                key = int(key)
            except ValueError:
                pass
            result[key] = value
        return result
    
class Database():

    def __init__(self):
        self.tables = []
        self.read = None
        self.path = None
        self.write = None
        self.bufferpool = []
        self.dirty = []
        self.bufferpool_limit = 32
        pass

    # Not required for milestone1
    def open(self, path):
        if (not os.path.isfile(path)):            
            f = open(path, 'w')
            self.path = path
            self.read = open(path, 'r')
        else:
            self.read = open(path, 'r')
            self.path = path
            num_tables = int(self.read.readline())
            for i in range(num_tables):
                info = self.read.readline().split()
                table = Table(info[0], int(info[1]), int(info[2]))
                table.db = self
                table.counter_base = int(info[3])
                table.counter_tail = int (info[4])
                with open(info[5]) as fi:
                    data = fi.read()
                table.page_directory = json.loads(data, object_pairs_hook=int_keys)
                
                #info[6] is the length of page_range_list
                for j in range(int(info[6])):
                    page_range = PageRange()
                    page_range.path = self.read.readline().strip()
                    page_range_file = open(page_range.path, 'r')
                    num_base = int(page_range_file.readline().strip())
                    for k in range(num_base):
                        base_page = []
                        base_page_path = page_range_file.readline().strip()
                        with open(base_page_path + '.json') as fi:
                            data = fi.read()
                        metadata = json.loads(data, object_pairs_hook=int_keys)
                        base_page.append(metadata[0])
                        base_page.append(metadata[1])
                        base_page.append(metadata[2])
                        base_page.append(metadata[3])
                        
                        
                        binary_file = open(base_page_path, 'rb')
                        for l in range(table.num_columns):
                            page = Page()
                            page.num_records = len(metadata[0])
                            page.data = bytearray(binary_file.read(4096))
                            base_page.append(page)
                        binary_file.close()
                        page_range.base_page_list.append(base_page)
                        
                    num_tail = int(page_range_file.readline().strip())
                    for k in range(num_tail):
                        tail_page = []
                        tail_page_path = page_range_file.readline().strip()
                        with open(tail_page_path + '.json') as fi:
                            data = fi.read()
                        metadata = json.loads(data, object_pairs_hook=int_keys)
                        tail_page.append(metadata[0])
                        tail_page.append(metadata[1])
                        tail_page.append(metadata[2])
                        tail_page.append(metadata[3])
                        
                        
                        binary_file = open(tail_page_path, 'rb')
                        for l in range(table.num_columns):
                            page = Page()
                            page.num_records = len(metadata[0])
                            page.data = bytearray(binary_file.read(4096))
                            tail_page.append(page)
                        binary_file.close()
                        page_range.tail_page_list.append(tail_page)
                        
                    table.page_range_list.append(page_range)
                    page_range_file.close()
                table.index.index_key()
                self.tables.append(table)
                        
        pass    

    def close(self):
        f = open(self.path, 'w')
        f.write(str(len(self.tables))+"\n")
        for i in range(len(self.tables)):
            table : Table = self.tables[i]
            page_directory_path = table.name + "_" + "page_directory.json"
            with open(page_directory_path, 'w') as file:
                json.dump(table.page_directory, file)
            
            info = table.name +"  " + str(table.num_columns) + " " + str(table.key)  + " "  + str(table.counter_base) + " " + str(table.counter_tail) + " " + page_directory_path + " " + str(len(table.page_range_list)) +  "\n" 
            f.write(info)
            
            for j in table.page_range_list:
                f.write(j.path+"\n")
          
        while len(self.bufferpool) > 0:
            self.write_page_range(self.bufferpool[0])
            self.bufferpool.pop(0)
        f.close()
        self.read.close()                

    def write_page_range(self, page_range: PageRange):
        path = page_range.path
        file = open(path, 'w')
        file.write(str(page_range.getNumBase())+"\n")
        
        for i in range(page_range.getNumBase()):
            base_page_path = path + " base page " + str(i)
            base_page = page_range.base_page_list[i]
            file.write(base_page_path + "\n")
            metadata = [base_page[0], base_page[1], base_page[2], base_page[3]]
            with open(base_page_path + ".json", 'w') as f:
                json.dump(metadata, f)
            
            binary_file = open(base_page_path, 'wb')
            for j in range(4,len(base_page)):
                binary_file.write(base_page[j].data)
            binary_file.close()
        
        file.write(str(page_range.getNumTail())+"\n")
        
        for i in range(page_range.getNumTail()):
            tail_page_path = path + " tail page " + str(i)
            tail_page = page_range.tail_page_list[i]
            file.write(tail_page_path + "\n")
            metadata = [tail_page[0], tail_page[1], tail_page[2], tail_page[3]]
            with open(tail_page_path + ".json", 'w') as f:
                json.dump(metadata, f)
            
            binary_file = open(tail_page_path, 'wb')
            for j in range(4,len(tail_page)):
                binary_file.write(tail_page[j].data)
            binary_file.close
        file.close()

    def page_range_in_bufferpool(self, pagerange): 
        for i in range(len(self.bufferpool)):
            if (pagerange.path == self.bufferpool[i].path):
                return i
        
        return -1
    
    def use_bufferpool(self, pagerange):
        index_in_bufferpool = self.page_range_in_bufferpool(pagerange)
        if  index_in_bufferpool != -1:
            return index_in_bufferpool
        if (len(self.bufferpool) < self.bufferpool_limit):
            self.bufferpool.append(pagerange)
            self.dirty.append(False)
            return len(self.bufferpool)-1
        elif (len(self.bufferpool) == self.bufferpool_limit):
            for i in range(len(self.bufferpool)):
                if (True): #this should check if any transaction uses the current page
                    dirty = self.dirty.pop(0)
                    evict_page  = self.bufferpool.pop(0)
                    
                    #write the evicted page onto the file
                    if dirty:
                        self.write_page_range(evict_page)
                    
                    self.dirty.append(False)
                    self.bufferpool.append(pagerange)
                    
                    return self.bufferpool_limit
        return -1
    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        table.db = self
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for i in range(len(self.tables)):
            if (self.tables[i].name == name):
                return self.tables.pop(i)

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for i in range(len(self.tables)):
            if (self.tables[i].name == name):
                return self.tables[i]
