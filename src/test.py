"""
Test functions return desired output.
"""
import re
from netCDF4 import Dataset

# Test utils functions
from utils import get_raw_file_name, get_agg_file_name, delete_file

def test_util_funcs():
    v = 'var'
    raw = get_raw_file_name(v)
    unique_num = re.search(r'\_(\d+).nc', raw)
    unique_num = int(unique_num.group(1))
    
    print("\nTesting raw and temporally aggregated file name creation:")
    true_agg = f"var_t1_s0_{unique_num}.nc"
    agg = get_agg_file_name(raw, t=1)
    if agg == true_agg:
        print(f"Aggregated file name is as expected:{agg}")
    else:
        print(f"Aggregated file name is {agg} =/= {true_agg}")
    print(f"Raw file name was: {raw}")

    print("\nTesting spatially aggregated file name creation:")
    true_agg = f"var_t0_s1_{unique_num}.nc"
    agg = get_agg_file_name(raw, t=0)
    if agg == true_agg:
        print(f"Aggregated file name is as expected:{agg}")
    else:
        print(f"Aggregated file name is {agg} =/= {true_agg}")

    file_name = "file_to_delete.nc"

    print("\nTesting file deletion:")
    print("File does not exist:")
    delete_file(file_name)

    print("File exists:")
    with Dataset(file_name, "w") as nc_file:
        pass
    delete_file(file_name)

    ### TODO: ADD TEST FOR save_csv, get_file_size
        # print(f"File size: {file_size_bytes} bytes ({file_size_mb:.2f} MB)")



if __name__ == "__main__":
    test_util_funcs()
