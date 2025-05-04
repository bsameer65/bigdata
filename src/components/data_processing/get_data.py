from components import logger
import os
import urllib.request as request
from components.constants import URL, root_dir, data_file
import requests
from tqdm import tqdm

class DataProcessing:
    def __init__(self):
        pass 
    
    # def get_file(self):
    #         os.makedirs(root_dir,exist_ok=True)
    #         logger.info("Looking for a file...")
    #         if not os.path.exists(data_file):
    #             logger.info("Start Downloading...")
    #             filename, headers = request.urlretrieve(
    #                 url=URL,
    #                 filename=data_file
    #             )
    #             logger.info(f"{filename} download! with following info: \n{headers}")
    #         else:
    #             logger.info(f"File already exists") 
    def get_file(self):
        os.makedirs(root_dir, exist_ok=True)
        chunk_size = 1024 * 1024
        # Start the request
        logger.info("Looking for a file...")
        if not os.path.exists(data_file):
            response = requests.get(URL, stream=True)
            total_size = int(response.headers.get('content-length', 0))
            num_chunks = total_size // chunk_size + 1

            # Write to file in chunks with progress
            with open(data_file, 'wb') as f, tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading") as pbar:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

            print(f"\n✅ Download complete! File saved to: {data_file}")
        else:
            logger.info("✅ File already exists. Skipping download.")