from email.mime import base
import json
import os
from lstore.table import Table, PageRange

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
            
        for i in range(len(self.bufferpool)):
            self.write_page_range(self.bufferpool[i])
            self.bufferpool.pop(i)
        
        f.close()
        self.read.close()                

    def write_page_range(self, page_range: PageRange):
        path = page_range.path
        file = open(path, 'w')
        file.write(str(page_range.getNumBase())+"\n")
        
        for i in range(page_range.getNumBase()):
            base_page_path = path + " base page " + i
            base_page = page_range.base_page_list[i]
            file.write(base_page_path + "\n")
            metadata = [base_page[0], base_page[1], base_page[2], base_page[3]]
            with open(base_page_path + ".json", 'w') as f:
                json.dump(metadata, f)
            
            binary_file = open(base_page_path, 'wb')
            for j in range(4,len(base_page)):
                binary_file.write(base_page[j])
            binary_file.close
        file.close()

    def page_range_in_bufferpool(self, pagerange): 
        for i in range(len(self.bufferpool)):
            if (pagerange.path == self.bufferpool[i].path):
                return i
        
        return -1
    
    def use_bufferpool(self, pagerange):
        if not self.page_range_in_bufferpool(pagerange) == -1:
            return True
        if (len(self.bufferpool) < self.bufferpool_limit):
            self.bufferpool.append(pagerange)
            self.dirty.append(False)
            return True
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
                    
                    return True
        return False
    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for i in (self.tables.__len__):
            if (self.tables[i].name == name):
                return self.tables.pop(i)

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for i in (self.tables.__len__):
            if (self.tables[i].name == name):
                return self.tables[i]
