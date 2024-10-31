import os
import pandas as pd


def create_random_file(size_bytes):
    return os.urandom(size_bytes)


trace = "all_traces/trace_drex_sc_0.999_8.csv"

df = pd.read_csv(trace)

for i,row in enumerate(df.iterrows()):
    data_size = int(row[1]['data_size']  * 1000 * 1000)
    print(data_size)
    obj = create_random_file(data_size)
    with open(f"/media/storage1/objects/{i}.obj", "wb+") as f:
        f.write(obj)
    print(f"written {i+1}/{df.size}")