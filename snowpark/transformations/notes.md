## Data Transformations
- Important for down stream tasks
- Involves
  - Cleaning
  - Filtering
  - Aggregating
  - Reformatting

### Get Count
```python
dataframe.count().show()
dataframe.distinct().count().show()
```

### Save Data As Table
- Use the following syntax

```python
dataframe.write.save_as_table("NAME_OF_TABLE")
```

### Joins
- Join data horizontally
- Dataframe syntax below
- The default join is an inner join
- Suffix is used to differentiate the id's being joined on below
```python
result_df = dataframe1.join(dataframe2, dataframe1.ID == dataframe2.ID, lsuffix="_LEFT", rsuffix="_RIGHT")
result_df = result_df.drop("ID_RIGHT")
final_df = result_df.join(dataframe3, result_df["ID_LEFT"] == dataframe3.ID)
```

### Unions
- Append data vertically
- Can be used to create new dataset that contains old and new data
- Use the following syntax
- There are two options
  - union and union_by_name
  - union_by_name will match the column names and ensure the data is properly appended using column names
  - union just appends vertically

```python
combined = new_data.union_by_name(old_data) # append data vertically
```

### Group By
- Use the following syntax

```python
dataframe.group_by("COLUMN_NAME").agg(max("COLUMN_NAME_AGG").alias("MY_ALIAS")).show()
```

### Sort
- Use the following syntax

```python
dataframe.sort(col("COL_NAME"), col("COL2_NAME"), ascending=[False, True])
```

### Drop Duplicates
- Use the following syntax

```python
dataframe.drop_duplicates()
```

### Convert Snowpark DataFrame To Pandas

```python
pandas_df = dataframe.to_pandas()
# or for memory efficiency use below
for batch in dataframe.to_pandas_batches():
    print(batch.shape)
```