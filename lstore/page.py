
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

    def write(self, value: int):
        if (self.has_capacity):
            start_offset = self.num_records*self.data_size
            end_offset = self.num_records*self.data_size + self.data_size
            self.data[start_offset : end_offset] = value.to_bytes(8, 'big')
            self.num_records += 1
            return True
        return False

    def read(self, offset):
        start_offset = offset*self.data_size
        end_offset = offset*self.data_size + self.data_size
        return int.from_bytes(self.data[start_offset : end_offset], 'big')