import pandas as pd

df0 = pd.read_parquet('../part-00000-51a8c104-676e-44ce-b9ca-ee4b08a80230-c000.snappy.parquet')
sorted_df0 = df0.sort_values(by=['_1', '_2'])
sorted_df0.to_csv('../part-00000-51a8c104-676e-44ce-b9ca-ee4b08a80230-c000.csv', index=False)

df0 = pd.read_parquet('../part-00001-51a8c104-676e-44ce-b9ca-ee4b08a80230-c000.snappy.parquet')
sorted_df0 = df0.sort_values(by=['_1', '_2'])
sorted_df0.to_csv('../part-00001-51a8c104-676e-44ce-b9ca-ee4b08a80230-c000.csv', index=False)

df0 = pd.read_parquet('../part-00002-51a8c104-676e-44ce-b9ca-ee4b08a80230-c000.snappy.parquet')
sorted_df0 = df0.sort_values(by=['_1', '_2'])
sorted_df0.to_csv('../part-00002-51a8c104-676e-44ce-b9ca-ee4b08a80230-c000.csv', index=False)

df0 = pd.read_parquet('../part-00003-51a8c104-676e-44ce-b9ca-ee4b08a80230-c000.snappy.parquet')
sorted_df0 = df0.sort_values(by=['_1', '_2'])
sorted_df0.to_csv('../part-00003-51a8c104-676e-44ce-b9ca-ee4b08a80230-c000.csv', index=False)
pass
