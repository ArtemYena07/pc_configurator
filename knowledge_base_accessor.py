from sqlalchemy import create_engine
from typing import Optional, Tuple, List, Dict
from itertools import chain


class DataBaseAccessor:
    def __init__(self):
        self.engine = create_engine('sqlite:///knowledge_base.sqlite')

    def get_cpu(self, core_number: int, frequency: int,
                city: Optional[str] = None) -> Optional[Tuple[int, int, int, int, int, str, str]]:
        with self.engine.connect() as con:
            if not city:
                rs = con.execute('SELECT core_number, frequency, price, producer_id, retailer_id, city, address FROM cpu_availability ' +
                                 'INNER JOIN cpu ON cpu_availability.cpu_id=cpu.id ' +
                                 'INNER JOIN retailer_point ON cpu_availability.retailer_point_id=retailer_point.id ' +
                                 'WHERE core_number >= ? AND frequency >= ? ' +
                                 'AND quantity >= 0 ORDER BY price',
                                 core_number, frequency)
                tup = rs.fetchone()
                if not tup:
                    return None
                core_number, frequency, price, producer_id, retailer_id, city, address = tup
                return core_number, frequency, price, producer_id, retailer_id, city, address
            else:
                rs = con.execute('SELECT core_number, frequency, price, producer_id, retailer_id, city, address FROM cpu_availability ' +
                                 'INNER JOIN cpu ON cpu_availability.cpu_id=cpu.id ' +
                                 'INNER JOIN retailer_point ON cpu_availability.retailer_point_id=retailer_point.id ' +
                                 'WHERE core_number >= ? AND frequency >= ? AND city = ? ' +
                                 'AND quantity >= 0 ORDER BY price',
                                 core_number, frequency, city)
                tup = rs.fetchone()
                if not tup:
                    return None
                core_number, frequency, price, producer_id, retailer_id, city, address = tup
                return core_number, frequency, price, producer_id, retailer_id, city, address

    def get_gpu(self, memory_type: str, memory_size: int,
                city: Optional[str] = None) -> Optional[Tuple[int, int, int, int, str, str]]:
        with self.engine.connect() as con:
            if not city:
                rs = con.execute('SELECT memory_size, price, producer_id, retailer_id, city, address FROM gpu_availability ' +
                                 'INNER JOIN gpu ON gpu_availability.gpu_id=gpu.id ' +
                                 'INNER JOIN retailer_point ON gpu_availability.retailer_point_id=retailer_point.id ' +
                                 'INNER JOIN memory_type ON gpu.memory_type_id=memory_type.id ' +
                                 'WHERE memory_size >= ? AND memory_type.name = ? ' +
                                 'AND quantity >= 0  ORDER BY price',
                                 memory_size, memory_type)
                tup = rs.fetchone()
                if not tup:
                    return None
                memory_size, price, producer_id, retailer_id, city, address = tup
                return memory_size, price, producer_id, retailer_id, city, address
            else:
                rs = con.execute('SELECT memory_size, price, producer_id, retailer_id, city, address  FROM gpu_availability ' +
                                 'INNER JOIN gpu ON gpu_availability.gpu_id=gpu.id ' +
                                 'INNER JOIN retailer_point ON gpu_availability.retailer_point_id=retailer_point.id ' +
                                 'INNER JOIN memory_type ON gpu.memory_type_id=memory_type.id ' +
                                 'WHERE memory_size >= ? AND memory_type.name = ?  AND quantity >= 0 ' +
                                 'AND city = ? ORDER BY price',
                                 memory_size, memory_type, city)
                tup = rs.fetchone()
                if not tup:
                    return None
                memory_size, price, producer_id, retailer_id, city, address = tup
                return memory_size, price, producer_id, retailer_id, city, address

    def get_ram(self, memory_type: str, memory_size: int, frequency: int,
                city: Optional[str] = None) -> Optional[Tuple[int, int, int, int, int, str, str]]:
        with self.engine.connect() as con:
            if not city:
                rs = con.execute('SELECT memory_size, frequency, price, producer_id, retailer_id, city, address FROM ram_availability ' +
                                 'INNER JOIN ram ON ram_availability.ram_id=ram.id ' +
                                 'INNER JOIN retailer_point ON ram_availability.retailer_point_id=retailer_point.id ' +
                                 'INNER JOIN memory_type ON ram.memory_type_id=memory_type.id ' +
                                 'WHERE memory_size >= ? AND memory_type.name = ? AND quantity >= 0 ' +
                                 'AND frequency >= ? ORDER BY price',
                                 memory_size, memory_type, frequency)
                tup = rs.fetchone()
                if not tup:
                    return None
                memory_size, frequency, price, producer_id, retailer_id, city, address = tup
                return memory_size, frequency, price, producer_id, retailer_id, city, address
            else:
                rs = con.execute('SELECT memory_size, frequency, price, producer_id, retailer_id, city, address FROM ram_availability ' +
                                 'INNER JOIN ram ON ram_availability.ram_id=ram.id ' +
                                 'INNER JOIN retailer_point ON ram_availability.retailer_point_id=retailer_point.id ' +
                                 'INNER JOIN memory_type ON ram.memory_type_id=memory_type.id ' +
                                 'WHERE memory_size >= ? AND memory_type.name = ? ' +
                                 'AND frequency >= ? AND city = ? AND quantity >= 0 ORDER BY price',
                                 memory_size, memory_type, frequency, city)
                tup = rs.fetchone()
                if not tup:
                    return None
                memory_size, frequency, price, producer_id, retailer_id, city, address = tup
                return memory_size, frequency, price, producer_id, retailer_id, city, address

    def get_ssd(self, memory_size: int, city: Optional[str] = None) -> Optional[Tuple[int, int, int, int, str, str]]:
        with self.engine.connect() as con:
            if not city:
                rs = con.execute('SELECT memory_size, price, producer_id, retailer_id, city, address  FROM ssd_availability ' +
                                 'INNER JOIN retailer_point ON ssd_availability.retailer_point_id=retailer_point.id ' +
                                 'INNER JOIN ssd ON ssd_availability.ssd_id=ssd.id ' +
                                 'WHERE memory_size >= ? AND quantity >= 0 ORDER BY price',
                                 memory_size)
                tup = rs.fetchone()
                if not tup:
                    return None
                memory_size, price, producer_id, retailer_id, city, address = tup
                return memory_size, price, producer_id, retailer_id, city, address
            else:
                rs = con.execute('SELECT memory_size, price, producer_id, retailer_id, city, address  FROM ssd_availability ' +
                                 'INNER JOIN retailer_point ON ssd_availability.retailer_point_id=retailer_point.id ' +
                                 'INNER JOIN ssd ON ssd_availability.ssd_id=ssd.id ' +
                                 'WHERE memory_size >= ? AND city = ? AND quantity >= 0 ORDER BY price',
                                 memory_size, city)
                tup = rs.fetchone()
                if not tup:
                    return None
                memory_size, price, producer_id, retailer_id, city, address = tup
                return memory_size, price, producer_id, retailer_id, city, address

    def get_list_of_cities(self) -> List[str]:
        with self.engine.connect() as con:
            rs = con.execute('SELECT DISTINCT city FROM retailer_point')
            lst = rs.fetchall()
            return list(chain.from_iterable(lst))

    def get_list_of_cpu_cores(self) -> List[int]:
        with self.engine.connect() as con:
            rs = con.execute('SELECT DISTINCT core_number FROM cpu')
            lst = rs.fetchall()
            return list(chain.from_iterable(lst))

    def get_list_of_cpu_freqs(self) -> List[int]:
        with self.engine.connect() as con:
            rs = con.execute('SELECT DISTINCT frequency FROM cpu')
            lst = rs.fetchall()
            return list(chain.from_iterable(lst))

    def get_list_of_gpu_mem_types(self) -> List[str]:
        with self.engine.connect() as con:
            rs = con.execute('SELECT DISTINCT memory_type.name ' +
                             'FROM gpu INNER JOIN memory_type ON ' +
                             'gpu.memory_type_id=memory_type.id')
            lst = rs.fetchall()
            return list(chain.from_iterable(lst))

    def get_list_of_gpu_mem_sizes(self) -> List[int]:
        with self.engine.connect() as con:
            rs = con.execute('SELECT DISTINCT memory_size FROM gpu')
            lst = rs.fetchall()
            return list(chain.from_iterable(lst))

    def get_list_of_ram_mem_types(self) -> List[str]:
        with self.engine.connect() as con:
            rs = con.execute('SELECT DISTINCT memory_type.name ' +
                             'FROM ram INNER JOIN memory_type ON ' +
                             'ram.memory_type_id=memory_type.id')
            lst = rs.fetchall()
            return list(chain.from_iterable(lst))

    def get_list_of_ram_mem_sizes(self) -> List[int]:
        with self.engine.connect() as con:
            rs = con.execute('SELECT DISTINCT memory_size FROM ram')
            lst = rs.fetchall()
            return list(chain.from_iterable(lst))

    def get_list_of_ram_freqs(self) -> List[int]:
        with self.engine.connect() as con:
            rs = con.execute('SELECT DISTINCT frequency FROM ram')
            lst = rs.fetchall()
            return list(chain.from_iterable(lst))

    def get_list_of_ssd_mem_sizes(self) -> List[int]:
        with self.engine.connect() as con:
            rs = con.execute('SELECT DISTINCT memory_size FROM ssd')
            lst = rs.fetchall()
            return list(chain.from_iterable(lst))

    def get_dict_of_producers(self) -> Dict[int, str]:
        with self.engine.connect() as con:
            rs = con.execute('SELECT DISTINCT id, name FROM producer')
            lst = rs.fetchall()
            return dict(lst)

    def get_dict_of_retailers(self) -> Dict[int, str]:
        with self.engine.connect() as con:
            rs = con.execute('SELECT DISTINCT id, name FROM retailer')
            lst = rs.fetchall()
            return dict(lst)
