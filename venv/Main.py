# 1.

# Because the data set contains over 50, 000 rows, you all need to read the data set into dataframes using
# 5,000 row chunks to ensure that each chunk consumes much less than 10 megabytes of memory. Across all of
# the chunks, become familiar with Eachcolumn's missing value counts Each column's memory footprint
#     The total memory footprint of  all of the chunks
#     combined
#     Which
#     column(s) we can drop because they aren 't useful for analysis
# 2.

# Identify the types for each column. Identify the numeric columns we can represent using more space efficient type
# For text columns:
#
# Analyze the unique value counts across all of the chunks to see if we can convert them to a numeric type.
# See if we clean clean any text columns and separate them into multiple numeric columns without adding any
# overhead when quering.
#
# Make your changes to the code from the last step so that the overall memory the data consumes stays under 10 megabytes.

# 3.
#
# Create and connect to a new SQLite database file
#
# Expand on the existing chunk processing code to export each chunk to a new table in the SQLite database
#
# Query the table and make sure the data types match up to what you had in mind for each column.

# 4.
#
# Repeat the tasks in this guided project using stricter memory constraints 1 mb
#
# Clean and analyze the other Crunchbase data sets from the https://github.com/datahoarder/crunchbase-october-2013
#
# Understand which columns the data sets share, and how the data sets are linked
#
# Create a relational database design that links the data sets together and reduces the overall disk space the database file consumes.
#
# Use pandas to populate each table in the database, create the appropriate indexes, and so on.

import pandas as pd

first_chunk = pd.read_csv('crunchbase-investments.csv', nrows=5000, encoding='Windows-1251')
data_iter = pd.read_csv('crunchbase-investments.csv', chunksize=5000, encoding='Windows-1251')

# print(first_chunk.head().info(memory_usage = 'deep'))
# the column below should be converted to int
# print(first_chunk['raised_amount_usd'])

# print(first_chunk.head())

miss_value = {}
total_chunks_mem = 0
cols_mem_foot = {}
col_types = {}

for chunk in data_iter:
    data_col = chunk.select_dtypes(include=['float', 'object'])
    cols = data_col.columns
    #     Each columns mem footprint
    total_chunks_mem += chunk.memory_usage(deep=True).sum() / (1024 * 1024)
    for col in cols:
        col_mem = chunk[col].memory_usage(deep=True) / (1024 * 1024)
        col_type = chunk[col].dtypes
        col_size = chunk[col].isnull().sum()
        if col not in miss_value:
            miss_value[col] = [col_size]
        else:
            miss_value[col].append(col_size)

        if col not in col_types:
            col_types[col] = [col_type]
        else:
            col_types[col].append(col_type)
            #     for col in miss_value:
#         u_concat = pd.concat(miss_value[col])
#         u_group = miss_value.groupby(u_concat.index).sum()
#         miss_value[col] = u_group
#         print(chunk[data_col].memory_usage(deep = True) / (1024*1024))
# print(miss_value)
# print(col_types)
# print(cols_mem_foot)
print('Total memory of all chunks: ', total_chunks_mem)


unique_textcols = {}
for chunk in data_iter:
    obj = chunk.select_dtypes(include=['objects'])
    obj_cols = obj.columns
    for obj_col in obj_cols:
        vals = obj[obj_col].value_counts()
        if vals not in unique_textcols:
            unique_textcols[obj_col] = [vals]
        else:
            unique_textcols[obj_col].append(vals)
unique_textcols
print(obj.columns)

