import pandas as pd

'''

Aggregating the file by industry and taking the mean of the predictors

'''

file_path = '/Users/fabrizioserafini/Desktop/senior_year/Senior Thesis/thesis_project/data/combined/combined.csv'
output_path = '/Users/fabrizioserafini/Desktop/senior_year/Senior Thesis/thesis_project/data/combined'
df = pd.read_csv(file_path)

# dropping ({'industry14_Corporate Tags', 'industry14_International bodies'}, {'Unnamed: 0.1'}) columns
df.drop(columns=['industry14_Corporate Tags', 'industry14_International bodies'], inplace=True)

# Dropping rows where 'absent_cdp_ghg_change_processed.csv' is set to 1
df_filtered = df[df['absent_cdp_ghg_change_processed.csv'] != 1]


# Identifying columns that indicate industry membership
industry_columns = [col for col in df.columns if col.startswith('industry14')]

# Grouping by industry and year and calculating the mean of variables for each group
grouped_mean_data_filtered = {}
for industry_col in industry_columns:
    # Grouping by industry and year
    grouped = df_filtered[df_filtered[industry_col] == 1].groupby('year')

    # Calculating mean of variables for each group
    mean_of_variables = grouped.mean()
    count_of_firms = grouped.size().to_frame(name='number_of_firms')

    # Combining the results
    grouped_mean_data_filtered[industry_col] = mean_of_variables.join(count_of_firms)

# Creating a single dataframe that includes the industry group as a column
combined_mean_df_list_filtered = []

for industry_col in industry_columns:
    industry_mean_df_filtered = grouped_mean_data_filtered[industry_col].copy()
    industry_mean_df_filtered['industry_group'] = industry_col
    combined_mean_df_list_filtered.append(industry_mean_df_filtered)

# Concatenating all the dataframes
combined_mean_df_filtered = pd.concat(combined_mean_df_list_filtered)

# Resetting the index to include the year as a column
combined_mean_df_filtered.reset_index(inplace=True)

# dropping the absent_cdp_ghg_change_processed.csv column
combined_mean_df_filtered.drop(columns=['absent_cdp_ghg_change_processed.csv'], inplace=True)

# output to path
combined_mean_df_filtered.to_csv(output_path + '/' + 'combined_industry14_mean.csv', index=False)
