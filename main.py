from knowledge_base_accessor import DataBaseAccessor
from typing import List, Optional, Union


def pick_from_list(lst: List[Union[str, int]], allow_na: bool = False) -> Optional[Union[str, int]]:
    for ind, item in enumerate(lst):
        print(f'{ind + 1}: {item}')
    rep = True
    item = None
    while rep:
        try:
            st = input()
            if st == '' and allow_na:
                rep = False
            else:
                num = int(st)
                item = lst[num - 1]
                rep = False
        except ValueError:
            print('Type a number')
        except IndexError:
            print('Type correct a number')
    return item


def main() -> None:
    db = DataBaseAccessor()

    cities = db.get_list_of_cities()
    print("Pick your city (skip if None of them)")
    city = pick_from_list(cities, allow_na=True)

    cpu_cores = db.get_list_of_cpu_cores()
    print("Pick minimal cpu core number")
    cpu_core_number = pick_from_list(cpu_cores)

    cpu_freqs = db.get_list_of_cpu_freqs()
    print("Pick minimal cpu frequency(Hz)")
    cpu_freq = pick_from_list(cpu_freqs)

    gpu_mem_types = db.get_list_of_gpu_mem_types()
    print("Pick gpu memory type")
    gpu_mem_type = pick_from_list(gpu_mem_types)

    gpu_mem_sizes = db.get_list_of_gpu_mem_sizes()
    print("Pick minimal gpu memory size(Gb)")
    gpu_mem_size = pick_from_list(gpu_mem_sizes)

    ram_mem_types = db.get_list_of_ram_mem_types()
    print("Pick ram memory type")
    ram_mem_type = pick_from_list(ram_mem_types)

    ram_mem_sizes = db.get_list_of_ram_mem_sizes()
    print("Pick minimal ram memory size(Gb)")
    ram_mem_size = pick_from_list(ram_mem_sizes)

    ram_freqs = db.get_list_of_ram_freqs()
    print("Pick minimal ram frequency(Hz)")
    ram_freq = pick_from_list(ram_freqs)

    ssd_mem_sizes = db.get_list_of_ssd_mem_sizes()
    print("Pick minimal ssd memory size(Gb)")
    ssd_mem_size = pick_from_list(ssd_mem_sizes)

    cpu_tup = db.get_cpu(cpu_core_number, cpu_freq, city)
    gpu_tup = db.get_gpu(gpu_mem_type, gpu_mem_size, city)
    ram_tup = db.get_ram(ram_mem_type, ram_mem_size, ram_freq, city)
    ssd_tup = db.get_ssd(ssd_mem_size, city)
    retailers_dict = db.get_dict_of_retailers()
    producers_dict = db.get_dict_of_producers()
    if not cpu_tup or not gpu_tup or not ram_tup or not ssd_tup:
        print("Cannot configure PC with such components")
        return

    core_number, frequency, cpu_price, producer_id, retailer_id, city, address = cpu_tup
    print(f'CPU:\n'
          f'    Core Numbers: {core_number}\n'
          f'    Frequency: {frequency} Hz\n'
          f'    Price: {cpu_price} UAH\n'
          f'    Producer: {producers_dict[producer_id]}\n'
          f'    Retailer: {retailers_dict[retailer_id]}, {city}, {address}\n')
    memory_size, frequency, ram_price, producer_id, retailer_id, city, address = ram_tup
    print(f'RAM:\n'
          f'    Memory size: {core_number} Gb\n'
          f'    Memory type: {ram_mem_type}\n'
          f'    Frequency: {frequency} Hz\n'
          f'    Price: {ram_price} UAH\n'
          f'    Producer: {producers_dict[producer_id]}\n'
          f'    Retailer: {retailers_dict[retailer_id]}, {city}, {address}\n')
    memory_size, gpu_price, producer_id, retailer_id, city, address = gpu_tup
    print(f'GPU:\n'
          f'    Memory size: {core_number} Gb\n'
          f'    Memory type: {gpu_mem_type}\n'
          f'    Frequency: {frequency} Hz\n'
          f'    Price: {gpu_price} UAH\n'
          f'    Producer: {producers_dict[producer_id]}\n'
          f'    Retailer: {retailers_dict[retailer_id]}, {city}, {address}\n')
    memory_size, ssd_price, producer_id, retailer_id, city, address = ssd_tup
    print(f'SSD:\n'
          f'    Memory size: {memory_size} Gb\n'
          f'    Price: {ssd_price} UAH\n'
          f'    Producer: {producers_dict[producer_id]}\n'
          f'    Retailer: {retailers_dict[retailer_id]}, {city}, {address}\n')
    print(f'Total price: {cpu_price + gpu_price + ram_price + ssd_price} UAH')


if __name__ == '__main__':
    main()
