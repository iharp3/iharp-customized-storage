import os
import subprocess

remote_path = "/export/scratch/uribe055/storage/sp"
local_folder = "/data/iharp-customized-storage/storage/test_data_pre_2024"

remote_user = "uribe055"
remote_host= "cs-spatial-502"

# Ensure the local folder exists
if not os.path.isdir(local_folder):
    raise ValueError(f"\tThe folder '{local_folder}' does not exist.")

# files = ["2m_temperature-2018.nc"]

try:
    # Construct the scp command with the -r flag
    scp_command = [
        "scp", "-r",
        local_folder,
        f"{remote_user}@{remote_host}:{remote_path}"
    ]
    # Execute the scp command
    subprocess.run(scp_command, check=True)
    print(f"\tSuccessfully transferred folder: \n\t\t{local_folder} \n\t\tto {remote_user}@{remote_host}:{remote_path}")
except subprocess.CalledProcessError as e:
    print(f"\tError transferring folder: {e}")
