import pandas as pd
import os
import gc  # Garbage Collector interface

# Your directory path
directory_path = '../data/CDP Cleaning/cleaned outputs'

counter1 = 0
counter2 = 0
# Get a list of file names
file_names = [f for f in os.listdir(directory_path) if f.endswith('.dta')]

# Initialize an empty DataFrame for the merged data
merged_df = pd.DataFrame()

# Loop through each file
for idx, file_name in enumerate(file_names):
    file_path = os.path.join(directory_path, file_name)
    # Read the Stata file
    temp_df = pd.read_stata(file_path)

    # Renaming 'reporting_year' to 'year' if necessary
    if 'reporting_year' in temp_df.columns:
        temp_df.rename(columns={'reporting_year': 'year'}, inplace=True)

    # Converting 'id' to integer type if it's an object type
    temp_df['id'] = temp_df['id'].astype(str)
    
    # Ensure 'year' is of the same type across all DataFrames
    temp_df['year'] = temp_df['year'].astype(str)

    # Merge with the main dataframe
    if idx == 0:  # If it's the first file, just set it to the merged_df
        merged_df = temp_df
    else:
        counter1 = merged_df.shape[0]
        merged_df = pd.merge(merged_df, temp_df, on=['id', 'year'], how='inner', suffixes=('', '_drop'))
        counter2 = merged_df.shape[0]
        print(f'Number of rows dropped due to inner join: {counter1 - counter2}')

        # reset counter 
        counter1 = 0
        counter2 = 0
        # Drop the '_drop' columns which are duplicates
        merged_df.drop([col for col in merged_df.columns if 'drop' in col], axis=1, inplace=True)
    
    # Delete the temporary dataframe to free up memory
    del temp_df
    # Invoke garbage collection
    gc.collect()

    # do a progress bar given that there are len files to process
    print(f'Processed {idx+1} of {len(file_names)} files')

    # break at 22
    if idx == 13:
        break


# to csv 
merged_df.to_csv('../data/CDP Cleaning/cleaned outputs/merged.csv', index=False)
