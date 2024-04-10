import pyarrow.parquet as pq
import pandas as pd
import pandasgui
import os

print(os.getcwd())

table = pq.read_table('project/data/01.parquet')
df = table.to_pandas()
pandasgui.show(df)