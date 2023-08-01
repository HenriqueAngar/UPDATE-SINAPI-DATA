# UPDATE-SINAPI-DATA

#ABOUT THE SCRIPT
It's a python script to inserts all information of prices of civil construction in Brazil from SINAPI information in a database.
The ETL process dowload all tables from all states of a specifc date into local files, extract all the information from a selection of files, and upload the data into a database.
Depending of your connection for the database the process might be slow, there are about 1.5 million of registers for each month, but don't worry just look for the prints to verify the process.

#how to use
To use the scripts you need only two things, first set the date that you want to insert. You need insert it in the call ATUSINAPI as int of a valid year and month, example june of 2023 as ATUSINAPI(202306).
The other part is configure your odbc string as function that bring this in dbcon. This depends in how you conect your database.

#THE DATABASE
For more information see the diagram, this structure fit's whith the uploads of information, and it's need create a procedure to sinc IDSIS of COMPOSIN AND INSUMOS IN COMPOAN.
The DB consists in three tables INSUMOS, a table for products and basic services. COMPOSIN a table of sinthetic compositions that is an assembly of compositions of products and services that results into something
as example what is necessary to build a wall. COMPOAN analitical compositions is basically an table tha have all details of services and products of an sinthetic composition in COMPOSIN.

#about keys
The SINAPI uses sames keys for each state and each date, to search you must need consider it. But you can create a procedure to sinc this with the primarys keys. You just neeed update COMPOAN whith IDSIS of INSUMOS 
and COMPOSIN, in COMPOAN columns COMPO, COMPO_REF and INSUMOS_REF
