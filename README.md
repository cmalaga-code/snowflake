# Snowflake

## Summary

- Founded 2012
- Managed SaaS (Software as a service)
- The Snowflake UI is called Snowsight
- Built on top of the major cloud providers
  - AWS
  - Azure
  - GCP
- Uses ANSI SQL (American National Standards Institute)
- Deploy
  - ML Models
  - Data Pipelines
  - Data Applications
- Data Warehousing
- Robust and scalable

## Snowpark Summary

- API that allows you to work with dataframe syntax
- Code written in supported languages are compiled down to ANSI SQL
- There is a snowpark package for the following languages
  - Python
  - Java
  - Scala
- Snowpark python API came out in June 2022
- Python has emerged as the preferred programming language for snowpark
- Allows for the use of Streamlit to build data applications
- Provides unified security and governance

## Snowpark Pros

1. Allows UDFs (User defined functions)
2. Allows for stored procedures to be written in Python or supported language
3. Supports pre-vetted open source packages through Anaconda (Allows for developers not to worry about dependencies)
4. Provides access to external data sources like AWS S3, Azure Blob Storage, Google Cloud Storage
5. Supports distributed processing for scalability
6. Handle real-time and batch data processing
7. Data centralization (keep the data in Snowflake. Store data and process data in Snowflake)