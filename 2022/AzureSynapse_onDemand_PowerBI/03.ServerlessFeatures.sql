

/* OFFSET/FETCH --> pagination */
select countries_and_territories, year, month, day, cases
from openrowset(
    bulk 'https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/ecdc_cases/latest/ecdc_cases.parquet',
    format = 'parquet') as rows
order by year, month, day 
OFFSET 40 ROWS
FETCH NEXT 10 ROWS ONLY;


/* string_agg --> JSON */
select top 10 geo_id, year, month, cases = '{' + string_agg(concat('"',day,'":',cases), ',')
                        within group (order by day asc) + '}'
from openrowset(
    bulk 'https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/ecdc_cases/latest/ecdc_cases.parquet',
    format = 'parquet') as rows
group by geo_id, year, month;


/* Session context */
CREATE OR ALTER VIEW cases
AS 
	SELECT * FROM openrowset
	(
		bulk 'https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/ecdc_cases/latest/ecdc_cases.parquet', 
		format = 'parquet'
	) as rows
    WHERE continent_exp = CAST(SESSION_CONTEXT(N'continent') AS VARCHAR(8000))
GO

exec sp_set_session_context 'continent', 'Europe';
SELECT TOP 10 * FROM cases

exec sp_set_session_context 'continent', 'Asia' 
SELECT TOP 10 * FROM cases

