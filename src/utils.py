"""
utils.py: Shared functions.
"""
import os
import time
import re
import csv

import config

def get_unique_num():
    return int(time.time() * 1000)

def get_raw_file_name(var):
    """
    Creates file name with unique number based on time.
    """
    u = get_unique_num()
    file_name = f"{var}_t0_s0_{u}.nc"

    return file_name

def get_agg_file_name(cur_file_name, t):
    """
    Adds one to the temporal (t == True) or spatial (t == False) count.
    """
    if t:
        # Increment count after '_t'
        file_name = re.sub(r'_t(\d+)', lambda m: f"_t{int(m.group(1)) + 1}", cur_file_name)
    else:
        # Increment count after '_s'
        file_name = re.sub(r'_s(\d+)', lambda m: f"_s{int(m.group(1)) + 1}", cur_file_name)
    
    return file_name

def modify_filename(cur_file_name, agg_type):
    """
    Replaces '.nc' with parameter agg_type.
    """
    return cur_file_name.replace('.nc', agg_type)

def get_data_path(file_name):

    return os.path.join(config.DATA_D, file_name)

def delete_file(file_path):
    try:
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"\tFile '{file_path}' has been deleted successfully.")
        else:
            print(f"\tThe path '{file_path}' does not exist or is not a file.")
    except Exception as e:
        print(f"\tAn error occurred while deleting the file: {e}")

def delete_files(filenames, directory):
    for filename in filenames:
        file_path = os.path.join(directory, filename)
        delete_file(file_path)

def save_csv(rows, file_path):
    """
    Saves csv file from list of dictionaries. Keys become column names in header.
    """
    headers = rows[0].keys() if rows else []
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

def save_list_to_csv(list, file_path):
    """
    Saves csv file from list of strings.
    """
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for l in list:
            writer.writerow([l])


def load_csv(file_path):
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = [row for row in reader]  # Each row is a dictionary
    return rows

def get_file_size(file_path):
    file_size_bytes = os.path.getsize(file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)

    return round(file_size_mb, 2)

def wait_for_file(file_path, timeout=1800, poll_interval=60):
    """
    Wait up to timeout seconds (1800sec=30mins) for file to exist.
    Check every poll_interval seconds.
    """
    start_time = time.time()
    while not os.path.exists(file_path):
        elapsed_time = time.time() - start_time
        if elapsed_time > timeout:
            print(f"Timeout reached. File '{file_path}' not created.")
            return False
        time.sleep(poll_interval)
    print(f"File '{file_path}' created.")
    return True

def get_min_max_of_array(arr):
    v_min = arr.min().compute().values.item()
    v_max = arr.max().compute().values.item()
    # print(f"\tMin:{v_min}, Max:{v_max}")

    return v_min, v_max

def compute_scale_and_offset_mm(mi, ma, n=16):
    vmin = mi
    vmax = ma
    # Stretch/compress data to the available packed range
    scale_factor = (vmax - vmin) / (2**n - 1)
    # Translate the range to be symmetric about zero
    add_offset = vmin + 2 ** (n - 1) * scale_factor
    # print(f"\tScale factor: {scale_factor}, add offset: {add_offset}")
    
    return scale_factor, add_offset

def get_scale_offset(arr):
    v_min, v_max = get_min_max_of_array(arr)
    scale, offset = compute_scale_and_offset_mm(v_min, v_max)
    return scale, offset, v_min, v_max