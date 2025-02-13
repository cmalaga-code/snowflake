-- query all the python packages in account
SELECT DISTINCT PACKAGE_NAME
FROM INFORMATION_SCHEMA.PACKAGES
WHERE LANGUAGE = 'python';

-- query for package name, version, and language available in account
SELECT PACKAGE_NAME, VERSION, LANGUAGE
FROM INFORMATION_SCHEMA.PACKAGES
WHERE PACKAGE_NAME = 'pandas' AND LANGUAGE = 'python';

