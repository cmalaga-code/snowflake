## Local Development Environment

### Summary

Snowpark only supports the following.
- Python 3.9
- Python 3.10
- Python 3.11

### Steps (anaconda)
1. Install anaconda
2. Create a virtual environment for development

```bash

conda create --name my_first_virt_env_conda --override-channels --channel https://repo.anaconda.com/pkgs/snowflake python=3.11
```
3. Activate virtual env

```bash

conda activate my_first_virt_env_conda # if want to deactivate use conda deactivate
conda info --envs # see which env you are in
```
4. Install dependencies

```bash

conda install --channel https://repo.anaconda.com/pkgs/snowflake snowflake-snowpark-python
conda install --channel https://repo.anaconda.com/pkgs/snowflake numpy pandas
```
5. Launch jupyter notebook (comes with anaconda)

```bash

jupyter notebook
```

6. On top right corner click on create a new notebook
7. Test with the following code

```python

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# Create a local session
session = Session.builder.config('local_testing', True).create()

# Create a DataFrame
data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
df = session.create_dataframe(data)

# Perform operations on the DataFrame
df = df.with_column("age_plus_one", col("age") + 1)

result = df.filter(df.NAME == "Alice")
result.show()

session.close()
```
### Steps (venv native to python)
1. Create a virtual env using venv

```bash

python3.11 -m venv .venv
```
2. Activate the virtual env

```bash

source .venv/bin/activate
```
3. Install packages

```bash

pip install snowflake-snowpark-python
```
4. Test with the following code (optional install jupyter notebook via pip install notebook and use a plugin)

```python

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# Create a local session
session = Session.builder.config('local_testing', True).create()

# Create a DataFrame
data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
df = session.create_dataframe(data)

# Perform operations on the DataFrame
df = df.with_column("age_plus_one", col("age") + 1)

result = df.filter(df.NAME == "Alice")
result.show()

session.close()
```