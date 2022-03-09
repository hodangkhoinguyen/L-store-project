"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from lstore.bPlusTree import BPlusTree
class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table
        
        for i in range(len(self.indices)):
            self.indices[i] = self.create_index(i)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        result = self.indices[column].locate(value)
        return result

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        result = self.indices[column].locate_range(begin, end)
        return result

        
            
    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number) -> BPlusTree:
        tree = BPlusTree(10)
        for page_range in self.table.page_range_list:
            for base_page in page_range.base_page_list:
                for i in range (len(base_page[0])):
                    tree.insert(base_page[4+column_number][i],base_page[1][i])
        return tree

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
