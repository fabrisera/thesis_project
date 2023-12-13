import pandas as pd
import numpy as np
import os

'''
After initial processing is done, each processed CSV has to be mapped to the year, id, isin
combination to ensure that then they can be merged together to create a desired training set

The operation performed is a left join, for every file a missingness indicator column is added if there 
are missing rows, and all missing rows are set to zeros. This technique will work best with a nonlinear model. 
'''

processed_path = '/Users/fabrizioserafini/Desktop/senior_year/Senior Thesis/thesis_project/data/processed'
mapping_file_path = '/Users/fabrizioserafini/Desktop/senior_year/Senior Thesis/thesis_project/data/processed/id_year_isin_mapping.csv'
output_path = '/Users/fabrizioserafini/Desktop/senior_year/Senior Thesis/thesis_project/data/mapped'

# name and dataframe? or just a list?
dfs = {}

files = os.listdir(processed_path)
relevant = []

# filterning relevant data
for file in files:
    if 'processed.csv' in file.split('_'):
        relevant.append(file)

# reading the relevant dfs
for df in relevant:
    print(df)
    read = pd.read_csv(processed_path + '/' + df)
    read.set_index(['id','year'], inplace=True)
    dfs[df] = read

mapping = pd.read_csv('/Users/fabrizioserafini/Desktop/senior_year/Senior Thesis/thesis_project/data/processed/id_year_isin_mapping.csv')
mapping.set_index(['id','year'], inplace=True)

for df in dfs.keys():
    mapped = mapping.join(dfs[df], on=['id', 'year']).reset_index()
    # check that there are missing values
    if mapped.isna().sum().sum() != 0:
        mapped['absent_' + df] = (mapped.isna().sum(axis=1) > 0 ).astype(int)
        mapped.replace(np.nan, 0, inplace=True)
    print(df, mapped.shape)
    mapped.to_csv(output_path + '/' + 'mapped_' + df)