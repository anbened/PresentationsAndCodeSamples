

/* Store query results to storage using serverless SQL pool in Azure Synapse Analytics */
USE [anbenedServeless];
GO

CREATE DATABASE SCOPED CREDENTIAL [SasTokenWrite_ADLS]
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
     SECRET = 'sp=racwdlmeo&st=2022-04-11T07:23:12Z&se=2022-10-13T15:23:12Z&spr=https&sv=2020-08-04&sr=c&sig=BBhZcY63GNKipsSdoVVULsL2G9pypMWl8w9HTbNNhvk%3D';
GO

CREATE EXTERNAL DATA SOURCE [MyDataSource_ADLS] WITH 
(						
    LOCATION = 'https://anbenedadls.blob.core.windows.net/cssecovid19timeseries', 
	CREDENTIAL = [SasTokenWrite_ADLS]
);
GO

CREATE EXTERNAL FILE FORMAT [ParquetFF_ADLS] WITH 
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO

CREATE EXTERNAL TABLE [dbo].[TestCETAS_ADLS] 
WITH 
(
        LOCATION = 'demoParquetADLS',
        DATA_SOURCE = [MyDataSource_ADLS],
        FILE_FORMAT = [ParquetFF_ADLS]
) AS
	select	
		ROW_NUMBER() OVER(ORDER BY dateconfirmed ASC) AS Row#,
		combined_key, cast(dateconfirmed as date) AS [Confirmed Date], 
		confirmed,
		CAST( ISNULL ( LAG(confirmed, 1) OVER (ORDER BY dateconfirmed ASC) , 0 ) as int) as confirmedPrevDay,

		CAST (confirmed as int) - CAST( ISNULL ( LAG(confirmed, 1) OVER (ORDER BY dateconfirmed ASC) , 0 ) as int) as confirmedDiff
	from
	(
		select *
		from 
			(
				select replace (combined_key,'"','') as combined_key, 
					[3/31/21], [4/1/21], [4/2/21], [4/3/21],
					[4/4/21], [4/5/21], [4/6/21], [4/7/21], [4/8/21], [4/9/21], [4/10/21]
				from timeseriescovid19confirmedUS 
				where UID != 'UID'
			) p
		UNPIVOT 
			(confirmed for dateconfirmed in ([3/31/21], [4/1/21], [4/2/21], [4/3/21],
					[4/4/21], [4/5/21], [4/6/21], [4/7/21], [4/8/21], [4/9/21], [4/10/21])) as unpvt
	) P
	where combined_key like 'Miami-Dade'
GO


select * from [dbo].[TestCETAS_ADLS] 
GO


/*
drop EXTERNAL TABLE [dbo].[TestCETAS_ADLS] 
drop EXTERNAL FILE FORMAT [ParquetFF_ADLS] 
drop EXTERNAL DATA SOURCE [MyDataSource_ADLS]
drop DATABASE SCOPED CREDENTIAL [SasTokenWrite_ADLS]
*/
