import pandas as pd
import re

df = pd.read_parquet('../part-00000-ed6e4b3a-e9b6-437b-8be6-a344e745dbc7-c000.snappy.parquet')
# print(df)
sorted_df = df.sort_values(by=['_1', '_2'])
print(sorted_df)
sorted_df.to_csv('../test.csv', index=False)
instance_file = '/home/masuda/uba1.7/University0_0.nt'
list1 = []
with open(instance_file, 'r') as input_file:
    instance_data = input_file.readlines()
    pattern = r'\s+(?=(?:[^"]*"[^"]*")*[^"]*$)'
    for line in instance_data:
        split_line = re.split(pattern, line)
        s = split_line[0]
        p = split_line[1]
        o = split_line[2]
        list0 = [s, o, p]
        list1.append(list0)
        pass
    df2 = pd.DataFrame(list1, columns=['_1', '_2', '_3'])
    sorted_df2 = df2.sort_values(by=['_1', '_2'])
    print(sorted_df2)
    sorted_df2.to_csv('../test2.csv', index=False)
    pass
pass
