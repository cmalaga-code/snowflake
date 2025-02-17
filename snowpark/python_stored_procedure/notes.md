## Python Stored Procedure
- Snowpark stored procedure is used for task execution and streams
- Can be executed on demand
- Can be parameterized
- Can interact with Snowflake objects
- Streams == real-time
- Tasks == batch-processing

```python
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import (
                        FloatType, 
                        StringType, 
                        IntegerType,
                        ArrayType
)

session_params = {
  "account": "<your_account>",
  "user": "<your_username>",
  "password": "<your_password>",
  "role": "<your_role>",
  "warehouse": "<your_warehouse>",
  "database": "<your_database>",
  "schema": "<your_schema>"
}

# Create the Snowpark session
session = Session.builder.configs(session_params).create()

# This function is the business logic. 
def my_function(session: Session, param1: float, param2: str) -> str:
    return "nice"

# register stored proc
session.sproc.register(
    func=my_function,
    name="my_function",
    return_type=StringType()
    input_types=[FloatType(), StringType()],
    is_permanent=True,
    replace=True,
    stage_location="@proc-stage-name/procedures",
    packages=['snowflake-snowpark-python']
)

session.close()
```

### Stored Procedures vs UDF

- Can be used together for more capabilities
- UDF is used for
  - Performing calculations
  - They require a value to be returned
  - Used to execute user defined logic on a dataset
- Stored procedure is used for
  - Data pipelines
  - Perform complex operations
  - No value needs to be returned
  - Admin tasks to maintain database
  - Can access the database
  
### Snowpark Project Structure
- Clone the repository below to get the official project structure
- git clone https://github.com/Snowflake-Labs/snowpark-python-template
