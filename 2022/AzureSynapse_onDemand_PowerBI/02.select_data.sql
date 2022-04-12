

/*
Source:
https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv
*/

SELECT TOP 5 * FROM dbo.timeseriescovid19confirmedUS
where UID != 'UID'
GO

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
