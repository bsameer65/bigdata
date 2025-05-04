import pandas as pd 

# df = pd.read_csv(,nrows=10000)

# print(df.shape)
# print(df.columns)
# print(df.isna().sum())
# df_copy = df[['Captured Time','Latitude','Longitude','Value','Unit','MD5Sum','Uploaded Time']]
# # for i in range(df_copy.shape[0]):
# #     print(df.iloc[i,:])
# print(df_copy['Unit'].value_counts())
# print(df[['Captured Time','Latitude',]].to_json())
# print(df_copy.head(30))
# print(df.dtypes)

# import pandas as pd
# import os

# # Settings
INPUT_FILE = r'D:\TU Hamburg\Semester 2\Big data\project\radiation_data\measurements.csv'        # <-- put your input file here
CHUNKS_FOLDER = 'cleaned_chunks'     # Folder to save small cleaned files
CHUNK_SIZE = 100000                 # 100k rows per chunk

# Make output folder if it doesn't exist
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Columns you really need (optional - improves speed)
columns_needed = ['Captured Time','Latitude','Longitude','Value','Unit','MD5Sum','Uploaded Time'] # change based on your needs

# Read file in chunks
reader = pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE, usecols=columns_needed)

for i, chunk in enumerate(reader):
    print(f"Processing chunk {i+1}...")

    # Drop rows with missing 'Captured Time'
    chunk = chunk.dropna(subset=['Captured Time'])

    # Sort inside chunk (small chunks are OK)
    chunk = chunk.sort_values('Captured Time')

    # Save this cleaned chunk to a Parquet file (much faster than CSV)
    chunk.to_parquet(f'{OUTPUT_FOLDER}/cleaned_chunk_{i}.parquet', index=False)

# # print("✅ Done processing all chunks!")
# import heapq
# import pyarrow.parquet as pq
# OUTPUT_FILE = 'fully_sorted.parquet'

# # List all the sorted files
# files = sorted(os.listdir(CHUNKS_FOLDER))

# # Open readers for all files
# dfs = [pd.read_parquet(os.path.join(CHUNKS_FOLDER, f), columns=['Captured Time', 'Latitude', 'Longitude']).iterrows() for f in files]

# # Prepare heap for merging
# heap = []

# # Initialize heap
# for idx, it in enumerate(dfs):
#     try:
#         i, row = next(it)
#         heapq.heappush(heap, (row['Captured Time'], idx, row))
#     except StopIteration:
#         continue

# # Final result buffer
# merged_rows = []

# while heap:
#     captured_time, idx, row = heapq.heappop(heap)
#     merged_rows.append(row)

#     try:
#         i, next_row = next(dfs[idx])
#         heapq.heappush(heap, (next_row['Captured Time'], idx, next_row))
#     except StopIteration:
#         continue

# # Create final dataframe
# final_df = pd.DataFrame(merged_rows)

# # Save to final sorted parquet
# final_df.to_parquet(OUTPUT_FILE, index=False)

# print("✅ All chunks merged and fully sorted!")

import pandas as pd

# Read parquet file
df = pd.read_parquet(r'D:\TU Hamburg\Semester 2\Big data\project\cleaned_chunks\cleaned_chunk_0.parquet')
print(df.shape)
print(df.head(30))