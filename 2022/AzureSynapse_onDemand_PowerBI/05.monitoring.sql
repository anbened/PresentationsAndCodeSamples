
/* SQL on Demand - MONITORING */

SELECT 
	ERH.status as [status],
	ERH.login_name as [login_name],
	ERH.start_time as [start_time],
	ERH.end_time as [end_time],
	ERH.total_elapsed_time_ms as [duration_ms],

	/* Data processed =  data scanned + data moved + data written */
	ERH.data_processed_mb as [data_processed_MB],

	/* Cost management for serverless SQL pool
	The amount of data processed is rounded up to the nearest MB per query. 
	Each query has a minimum of 10 MB of data processed. */
	CASE WHEN ERH.data_processed_mb < 10 THEN 10 ELSE ERH.data_processed_mb END as [data_pricing_MB],
	
	cast(ERH.total_elapsed_time_ms/1000.0 as decimal(12,2)) as [duration_sec],
	ERH.command as [statement],
	ERH.query_text as [command]	
FROM sys.dm_exec_requests_history ERH
ORDER BY ERH.start_time desc


/* Please note that price per TB depends on region */
DECLARE @EuroPerTB decimal(5,2) = 4.22

SELECT 
	ERH.login_name as [login_name],
	month(ERH.start_time) as [month],
	sum(ERH.data_processed_mb) as [SUM data_processed_MB],
	sum(CASE WHEN ERH.data_processed_mb < 10 THEN 10 ELSE ERH.data_processed_mb END) as [SUM data_pricing_MB],
	cast(sum(CASE WHEN ERH.data_processed_mb < 10 THEN 10 ELSE ERH.data_processed_mb END)/1024.0 as decimal(12,3)) as [SUM data_pricing_GB],

	cast( (cast(sum(CASE WHEN ERH.data_processed_mb < 10 THEN 10 ELSE ERH.data_processed_mb END)/1024.0 as decimal(15,5))/1024.0)*@EuroPerTB as decimal(15,5)) as [Monthly € Billing]
FROM sys.dm_exec_requests_history ERH
GROUP BY ERH.login_name, month(ERH.start_time)
ORDER BY ERH.login_name, month(ERH.start_time)

