## Data Pipeline

### Traditional vs Modern Data Pipelines

- Traditional data pipelines can have a lot of technical debt and complex integration
- Modern data pipelines provide the benefit of a unified platform
- Snowpark is natively integrated with Snowflake
- Snowflake encourages the use of ELT (Extract Load Transform) instead of ETL (Extract Transform Load)
    - ELT ensures that the processing occurs inside of Snowflake making it efficient and scalable
- Data Preparation
    - Get the data ready
    - Join tables .. etc
    - Snowpark allows you to create multiple stored procedures for data preparation

```python
from snowflake.snowpark import Session
from snowflake.snowpark.types import IntegerType, StringType
import snowflake

# extract data (join data, read data)
def prep_stored_proc(session: Session) -> StringType():
		# load data into df (lazy)
  table1 = session.table("customer")
  table2 = session.table("orders")
  # join data
  final = table2.join(table1, table2["CUSTOMER_ID"] == table1["ID"])
  # load data into snowflake
  final.write.save_as_table("FINAL_ANALYSIS")
  return "LOADED DATA"
 
 # Transform data (filter, remove or include properties, group or pivot data)
def transform_stored_proc(session: Session) -> StringType():
  final = session.table("FINAL_ANALYSIS")
  sub = final.select("CUSTOMER_NAME", "ORDER_NAME", "TOTAL_AMOUNT")
  sub_pivot = sub.pivot("ORDER_NAME", ["tv", "fridge", "playstation 5"]).sum("TOTAL_AMOUNT")
  sub_pivot.write.save_as_table("ORDER_TOTAL_PIVOT")
  return "CREATED PIVOT"

# Load clean/qa data (remove anomoloies, ensure quality by filtering)
def clean_stored_proc(session: Session):
  final_pivot = session.table("ORDER_TOTAL_PIVOT")
  final_pivot_drop_na = final_pivot.dropna()
  final_pivot.write.save_as_table("ORDER_TOTAL_PIVOT_QA")
  return "CLEANED DATA"
		
	  
    

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
   
prep_proc = session.sproc.register(
	 func=prep_stored_proc,
	 replace=True,
	 return_type=StringType(),
	 stage_location="@stage/proc-name",
	 packages=["snowflake-snowpark-python"]
)

transform_proc = session.sproc.register(
	 func=transform_stored_proc,
	 replace=True,
	 return_type=None,
	 stage_location="@stage/proc-name",
	 packages=["snowflake-snowpark-python"]
)

qa_proc = session.sproc.register(
	 func=clean_stored_proc,
	 replace=True,
	 return_type=StringType(),
	 stage_location="@stage/proc-name",
	 packages=["snowflake-snowpark-python"]
)
```

### Snowflake Tasks

- Can execute three types of code
  - Single SQL statements
  - Call to stored procedures
  - Procedural logic using Snowflake scripting
- Only one instance of the task runs at a time
- If a task is still executing and the scheduled execution is coming up then the scheduled execution will be skipped
- Tasks can be serverless (snowflake determines the compute needed based on historical data) or compute can be allocated manually
- **Task Graphs** (Use this for pipeline .. monitor via Snowsight)
  - Allow for the organization of tasks based on the dependency (there can be multiple dependencies)
  - Workflow management
  - Task graph max of 1000 tasks total and max 100 predecessors and 100 child tasks
- https://docs.snowflake.com/en/developer-guide/snowflake-python-api/tutorials/tutorial-2

### Logging

- First need to set the log level in the session

```python
session.sql("""ALTER session SET LOG_LEVEL = INFO;""").show()
```

- **All logs go to the event object in Snowflake**
- Example below

```python
import logging
from snowflake.snowpark import Session
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.types import StringType

def clean_proc(session: Session):
    logger = logging.getLogger("<LOGGER_NAME>")
    try:
        logger.info("Pipeline for data prep started")
        logger.info("Begin loading data")

        data1 = session.table("<TABLE1>")
        data2 = session.table("<TABLE2>")
    
        final = data1.join(data2, on="id") # verify syntax .. just example

        logger.info("Finished joining data")
        logger.info("Finished")

        final.write.save_as_table("<TABLE_NAME>")
        logger.info("Loaded data")
        return "LOADED DATA"
    except Exception as e:
        logger.error("Logging ERROR")
        logger.error(e)
        return "ERROR"

session_params = {
  "account": "<your_account>",
  "user": "<your_username>",
  "password": "<your_password>",
  "role": "<your_role>",
  "warehouse": "<your_warehouse>",
  "database": "<your_database>",
  "schema": "<your_schema>"
}

session = Session.builder.configs(session_params).create()
	
session.sproc.register(
	func=clean_proc,
	return_type=StringType(),
	input_types=[],
	is_permanent=True,
	name="CLEAN_PROC",
	replace=True,
	stage_location="@STAGE_NAME"
)
	

```

- Query logs from the event table below
```python
session.sql("""
	SELECT RECORD['severity_text'] AS SEVERITY,
		VALUE AS SEVERITY_MESSAGE,
	FROM MY_EVENTS
	WHERE SCOPE['name'] = '<LOGGER_NAME>' AND
	RECORD_TYPE = 'LOG';
""").collect()
```