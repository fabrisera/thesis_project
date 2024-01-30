import pandas as pd
import numpy as np
import os

'''
Combining all the datasets together
'''


mapping_file_path = '/Users/fabrizioserafini/Desktop/senior_year/Senior Thesis/thesis_project/data/mapped'
output_path = '/Users/fabrizioserafini/Desktop/senior_year/Senior Thesis/thesis_project/data/combined'

# name and dataframe? or just a list?
dfs = {}

files = os.listdir(mapping_file_path)
relevant = []

# filterning relevant data
for file in files:
    if 'processed.csv' in file.split('_'):
        relevant.append(file)

# reading the relevant dfs
for df in relevant:
    print(df)
    read = pd.read_csv(mapping_file_path + '/' + df)
    read.set_index(['id'], inplace=True)
    dfs[df] = read

# Concatenate all DataFrames along the columns
combined_df = pd.concat(dfs, axis=1)

# Drop duplicate columns
combined_df = combined_df.loc[:,~combined_df.columns.duplicated()]

# Reset the index if needed
combined_df.reset_index(inplace=True)

combined_df.columns = combined_df.columns.get_level_values(1)

combined_df = combined_df.iloc[:, 2:]

# Drop duplicate columns
combined_df = combined_df.loc[:,~combined_df.columns.duplicated()]

# Reset the index if needed
combined_df.reset_index(inplace=True)

combined_df.drop(columns=['index', 'isin', 'Unnamed: 0'], inplace=True)

# output to path
combined_df.to_csv(output_path + '/' + 'combined.csv')