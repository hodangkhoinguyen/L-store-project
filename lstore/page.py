
class Page:

    def __init__(self):
        self.num_records = 0
        self.capacity = 4096
        self.data_size = 8
        self.data = bytearray(self.capacity)

    def has_capacity(self):
        if self.num_records * self.data_size < self.capacity:
            return True
        
        return False

    def write(self, value):
        self.num_records += 1
        self.data[self.num_records]
        pass

