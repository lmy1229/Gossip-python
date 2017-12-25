
class BasicCache():
    """Base class of all kinds of caches."""
    def __init__(self, capacity):
        self.capacity = capacity
        
    def set(self, key, value):
        raise Exception('Not Implemented')

    def get(self, key):
        raise Exception('Not Implemented')

class LRUCache(BasicCache):
    """Cache using Least Recently Used (LRU) algorithm"""
    def __init__(self, capacity):
        super().__init__(capacity)
        self.cache = {}
        self.used_list = []

    def set(self, key, value):
        if key in self.cache:
            self.used_list.remove(key)
        elif self.capacity > 0 and len(self.cache) == self.capacity:
            self.cache.pop(self.used_list.pop(0))
        self.used_list.append(key)
        self.cache[key] = value

    def get(self, key):
        if key in self.cache:
            if not key == self.used_list[-1]:
                self.used_list.remove(key)
                self.used_list.append(key)
            return self.cache[key]
        else:
            return None
        