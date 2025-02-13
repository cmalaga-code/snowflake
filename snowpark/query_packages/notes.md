## Query Packages

Snowflake allows the user to query for existing packages. The user can filter for language, version, and package name.

### Custom packages

If the developer needs custom packages in a worksheet or notebook follow the steps below.

1. Login
2. Data > Databases
3. Select the desired database and schema to create the internal stage
4. Select create, and select stage
5. Select Snowflake managed
6. Enter name of new stage and comment and then click create
7. Move to Data > Database > Schema (where internal stage was created)
8. Click "Files" to add the package files and then click upload

The developer can access stage packages in notebooks and worksheets by clicking on packages and then clicking on stage packages

