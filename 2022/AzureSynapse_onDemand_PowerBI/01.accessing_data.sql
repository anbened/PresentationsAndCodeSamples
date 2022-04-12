

-- Create master key in databases with some password (one-off per database)
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'hF%Rx%*B4-B8Nn4R'
GO


CREATE DATABASE SCOPED CREDENTIAL [dbScopedCredential_Ferrero]
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
     SECRET = 'sp=rl&st=2021-11-09T09:05:48Z&se=2022-08-19T16:05:48Z&spr=https&sv=2020-08-04&sr=c&sig=ano0x35j8gsTfHBoimOJRB9y96n70o9ei5jK%2BMDjXeo%3D';
GO

CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat_Ferrero] WITH ( FORMAT_TYPE = PARQUET)
GO

CREATE EXTERNAL DATA SOURCE externalDS_Ferrero
WITH 
(    
	LOCATION   = 'https://anbenedsynapsene.dfs.core.windows.net/anbenedsynapsene/',
	CREDENTIAL = [dbScopedCredential_Ferrero] 
)
GO


CREATE EXTERNAL TABLE dbo.userData 
( 
	[M] int,
	[N] int,
	[P] varchar(8000)
)
WITH 
( 
	LOCATION = 'dataAggregatedByMonthParquet/*.parquet',
    DATA_SOURCE = [externalDS_Ferrero],
    FILE_FORMAT = [SynapseParquetFormat_Ferrero] 
)
GO


select * from dbo.userData
select count(*) from dbo.userData
select AVG(n) from dbo.userData
GO


/*
drop EXTERNAL TABLE dbo.userData 
drop EXTERNAL DATA SOURCE externalDS_Ferrero
drop EXTERNAL FILE FORMAT [SynapseParquetFormat_Ferrero]
drop DATABASE SCOPED CREDENTIAL [dbScopedCredential_Ferrero]
*/
