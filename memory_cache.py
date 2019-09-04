import sys
import time
import threading


class MemoryCache:
    def __init__(self, ttl: int = 5 * 60, max_size: int = 10 * 1024 * 1024,
                 auto_clean_by_dead_time: bool = True, auto_clean_by_max_size: bool = False):
        """
         :param ttl: The effective time
                        unit: s
                        default: 5 min
         :param max_size: Maximum memory value that can withstand
                        unit: bytes
                        default: 10 Mb
        """
        self.__cache = {}  # Specific cached data
        self.__ttl_cache = {}  # A list of all keys that should be cleared for each end time
        self.__lock = threading.Lock
        self.__size = 0

        self.__auto_clean_by_max_size = auto_clean_by_max_size

        if ttl <= 0:
            ttl = 5 * 60
        self.__ttl = ttl

        if max_size <= 0:
            max_size = 10 * 1024 * 1024
        self.__max_size = max_size

        # Periodically clean up stale data
        if auto_clean_by_dead_time:
            clean_timer = threading.Thread(None, self.__clean_timer, None)
            clean_timer.daemon = True
            clean_timer.start()

    def set_ttl(self, ttl: int = 5 * 60):
        if ttl <= 0:
            ttl = 5 * 60
        self.__ttl = ttl

    def set_max_size(self, max_size: int = 10 * 1024 * 1024):
        if max_size <= 0:
            max_size = 10 * 1024 * 1024
        self.__max_size = max_size

    def __clean_timer(self):
        while True:
            time.sleep(10 * 60)
            self.clean_by_dead()

    def __get_now_time(self) -> int:
        return int(time.time())

    def __before(self):
        if self.__size > self.__max_size:
            self.clean_by_dead()

    def add_node(self, key: (int, float, str), value, ttl: int = 0, dead_time: int = 0) -> bool:
        """
        Add a cache node
        :param key:
        :param dead_time:
        :param ttl: The effective time
                        unit: s
        :param value:
        :param dead_time: Dead time

        """
        try:
            if self.__size > self.__max_size:
                self.clean_by_dead()

            key = str(key)
            node = {
                "key": key,
                "value": value,
                "dead_time": self._get_real_dead_time(ttl, dead_time),
                "size": 0,
            }
            node["size"] = sys.getsizeof(node)

            if self.__del_by_key(key):
                self.__cache[key] = node
                if node.get("dead_time") not in self.__ttl_cache:
                    self.__ttl_cache[node.get("dead_time")] = []

                self.__size += node["size"]
                self.__ttl_cache[node.get("dead_time")].append(key)
                return True
            else:
                return False
        except:
            return False
        finally:
            if self.__auto_clean_by_max_size:
                self.clean_by_size(self.__max_size)

    def add_node_with_dead_time(self, key: (int, float, str), value, dead_time: int = 0) -> bool:
        return self.add_node(key, value, 0, dead_time)

    def add_node_with_ttl(self, key: (int, float, str), value, ttl: int = 0) -> bool:
        return self.add_node(key, value, ttl)

    def get_node(self, key: (int, float, str)):
        try:
            key = str(key)
            if key in self.__cache:
                node = self.__cache.get(key)
                if node.get("dead_time") > self.__get_now_time():
                    return node.get("value")
                else:
                    self.__del_by_key(key)
            return None
        except:
            return None

    def del_node(self, key: (int, float, str)) -> bool:
        key = str(key)
        return self.__del_by_key(key)

    def _get_real_dead_time(self, ttl: int = 0, dead_time: int = 0) -> int:
        if ttl != 0:
            return ttl + self.__get_now_time()
        elif dead_time != 0:
            return dead_time
        else:
            return self.__ttl + self.__get_now_time()

    def clean_by_dead(self):
        try:
            dead_time_list = self.__ttl_cache.keys().sort()
            now_time = self.__get_now_time()
            for dead_time in dead_time_list:
                # If the first uninvalidated one is found, exit clean
                if dead_time > now_time:
                    break

                for key in self.__ttl_cache[dead_time]:
                    self.__del_by_key(key)

                self.__ttl_cache.pop(dead_time)
        except:
            pass

    def clean_by_size(self, size: int = 0):
        # First clean up the expired key
        self.clean_by_dead()

        dead_time_list = self.__ttl_cache.keys().sort()
        while self.__size <= size:
            for dead_time in dead_time_list:
                for key in self.__ttl_cache[dead_time]:
                    self.__del_by_key(key)
                    break
                self.__ttl_cache.pop(dead_time)

    def __del_by_key(self, key: str) -> bool:
        try:
            # If it already exists, update the clear time
            if key in self.__cache:
                self.__ttl_cache[self.__cache.get(key).get("dead_time")].remove(key)
            if key in self.__cache:
                dead_node = self.__cache.pop(key)
                self.__size -= dead_node["size"]
            return True
        except:
            return False
