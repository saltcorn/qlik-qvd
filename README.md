# qlik-qvd

Provides the import_qvd_file function, which takes as argumnets:

- a filename (a file in the file store) 
- optionally a table name (otherwise it will use the name found in the QVD file)

and when run:

1. If the table does not exist, creates it with the fields in the QVD file
2. Imports the rows in the QVD file into the table