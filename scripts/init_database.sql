/* ====== ==============

=============================
  This is the beginnings of my data warehouse:

  Script created to check for if the database 'DataWarehouse' already exists. If it does, this database is dropped and recreated. If it doesn't exist, the database is made regardless.
  Following database creation, 3 schemas are created 'bronze', 'silver', and 'gold'.

  CAUTION: Only run this script if you either: checked already to see if there is a database and one does not exist, you've checked and backed up the files, or you do not care about files in the database if it does exist. 
*/ =====================

--- Create Database 'DataWarehouse'

  
  
USE master;
GO

--- Drop and create 'DataWarehouse' 
IF EXISTS (SELECT  1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

--- Database creation
CREATE DATABASE DataWarehouse;

USE DataWarehouse;


--- Creation of all layers of schemas required for the project
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
