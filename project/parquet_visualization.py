import pyarrow.parquet as pq
import pandas as pd
import pandasgui
import os

print(os.getcwd())

table = pq.read_table('data/matomo_log_link_visit_action/2024/04/04.parquet')
df = table.to_pandas()
pandasgui.show(df)