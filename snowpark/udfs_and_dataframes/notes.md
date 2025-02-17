## UDF (User Defined Function) & DataFrames

### DataFrames

- Snowpark DataFrames are more efficient than Pandas DataFrames
- Snowpark DataFrame is an abstraction that allows the user to work with a DataFrame syntax similar to Spark and Pandas
- Snowpark DataFrames use lazy evaluation (operations are executed when requesting the result)
  - The .collect() method executes the operations and returns the result as a list of rows
- Users can use Snowpark DataFrames locally by creating a session by using the Session class

```python

from snowflake.snowpark import Session

connection = {
    "account": "<account-name>",
    "user": "<username>",
    "password": "<password>",
    "role": "<role>",
    "warehouse": "<wharehouse>",
    "database": "<database>",
    "schema": "<schema>"
}

session = Session.builder.configs(connection_parameters).create()
df = session.table("POST")

session.close() # good practice to close session
```

- Instructions in the DataFrame API are compiled back down to SQL
- With a session a user can execute raw SQL
- Remember to close the session with session.close()

```python

from snowflake.snowpark import Session

session = Session.builder.configs('local_testing').create()

session.sql('SELECT * FROM POST').collect()
session.close()
```
```python
# Python worksheet
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session): 
    df_example = session.table("POST").filter(col('"upvote"') == 0)
    result = df_example.select(col('"upvote"').alias("UPVOTE"), 
													    col('"id"').alias("ID"))
    result.show()
    return result
```

### UDF
- User defined functions
- Registered UDFs can be used in RAW SQL or in the DataFrame syntax

```python

from snowflake.snowpark.types import FloatType, StringType, IntegerType, ArrayType
from snowflake.snowpark.functions import col, call_udf
from snowflake.snowpark import Session

def square_function(x: int):
	return x ** 2
	
session = Session.builder.configs('local_testing').create()
	
session.udf.register(
	func=square_function,
	return_type=IntegerType(),
	input_types=[IntegerType()],
	is_permanent=True,
	name='square_function',
	replace=True,
	stage_location='@stage_name'
)

session.sql("SELECT SQUARE_FUNCTION(AMOUNT) AS RESULT FROM POST;")

session.close()
```
```python
df.with_column("RESULT", call_udf("SQUARE_FUNCTION", col("AMOUNT"))).show()
```

### UDF vs UDTF
- UDF and UDTF are invoked once per row
- UDTF can return multiple rows as output for each row
- UDTF has additional optional parameters
- UDTF is a type of UDF

```python
 from snowflake.snowpark.types import StructType, StructField
 from snowflake.snowpark.types import FloatType, StringType, IntegerType, ArrayType
 from snowflake.snowpark import Session
 
 
 session = Session.builder.configs('local_testing').create()
 session.udtf.register(
	 handler='<Name of main Python class>',
	 output_schema=StructType([StructField("<column_name>", StringType), 
														 StructField("<column_name>", FloatType)]),
	 input_types=[IntegerType],
	 is_permanent=True,
	 name='my_udtf_function',
	 replace=True,
	 stage_location='@stage-name'
	 
)
session.close()
```
### Vectorized UDFs
- Pandas DataFrame UDFs
- Decorator
- Define python function that will receive batches of input rows
- Returns collection of Pandas Series
- Parallelize operations making it more effective
- Works on top of Pandas DataFrames
```python
import pandas as pd
from snowflake.snowpark.functions import pandas_udf
from snowlfake.snowpark.types import PandasSeriesType, IntegerType
from snowflake.snowpark import Session

@pandas_udf(name='add_number', stage_location='@the-stage'
						, input_type=[PandasSeriesType(IntegerType())]
						, return_type=PandasSeriesType(IntegerType())
						, is_permanent=True
						, replace=True)
def add_number(col: pd.Series, number) -> pd.Series:
	return col + number
	
session = Session.builder.configs('local_testing').create()
df = session.table("POST")
df.with_column('NEW_UPVOTE', add_number(col('"upvote"'))).show() 
session.close()
```