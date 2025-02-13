## Python Stored Procedure
- Can be executed on demand
- Can be parameterized
- Interact with Snowflake objects
- Snowpark stored procedure is used for task execution and streams
- Streams == real-time
- Tasks == batch-processing

```python
from snowflake.snowpark.types import FloatType, StringType, IntegerType, ArrayType

def my_function(session, param1, param2):
    return "nice"

session.sproc.register(
    func=my_function,
    return_type=StringType()
input_types=[],
is_permanent=True,
replace=True,
stage_location="@proc-stage-name"
)
```

### Stored Procedures vs UDF

- Can be used together for more capabilities
- UDF is used for
  - Performing calculations
  - They require a value to be returned
  - Used to execute user defined logic on a dataset
- Stored procedure is used for
  - Perform complex operations
  - No value needs to be returned
  - Admin tasks to maintain database
  - Can access the database
### Snowpark Project Structure

git clone https://github.com/Snowflake-Labs/snowpark-python-template