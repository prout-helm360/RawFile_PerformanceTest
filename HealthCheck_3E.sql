-- Bimal Dave
-- Elite, a Thomson Reuters Business
-- 2015-04-28
-- Collection of scripts to perform a 3E SQL Server Health Check 

-- 3E SPECIFIC

EXEC TE3EDBChecker_sp 'All' 

-- List 3E Scheduled Tasks
select top 100 NxNtfTask.Name,NxNtfSched.Name, NxNtfSched.Description, NxNtfSched.archetypecode,NxNtfSchedule.lastruntime,  NxNtfSchedule.nextruntime
from NxNtfSchedule, NxNtfTask, NxNtfSched
where NxNtfTask.NxNtfTaskID = NxNtfSchedule.task
and NxNtfSched.NxNtfSchedID = NxNtfSchedule.schedule
order by NxNtfTask.Name

-- Health Monitor
-- If running for the first time, you need to uncomment the next section to create these stored procedures

--START creation of stored procedures
/*
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create procedure sp_3ehealthmonitorbigreports
@starttime datetime,
@endtime datetime,
@rowcnt integer
as
SELECT 'Reports' as Type,
n2t2.AppObjectTypeCode + ' - ' + n1t1.AppObjectCode as Code, 
n0t0.NetworkAlias as NetworkUser,
n0t0.TimeStamp as Timestamp, 
convert(int,replace(n0t0.Details, 'Rows Returned = ','')) as Details,
n0t0.Server as Server
FROM NxReportLog AS n0t0 WITH ( nolock ) 
JOIN NxFWKAppObject AS n1t1 WITH ( nolock ) 
JOIN NxFWKAppObjectType AS n2t2 WITH ( nolock ) 
ON ( n1t1.AppObjectTypeId ) = ( n2t2.NxFWKAppObjectTypeID ) 
ON ( n0t0.AppObjectID ) = ( n1t1.NxFWKAppObjectID ) 
where convert(int,replace(n0t0.Details, 'Rows Returned = ','')) > @rowcnt
and n0t0.TimeStamp >= convert(datetime,@starttime,20)
and n0t0.TimeStamp <= convert(datetime,@endtime,20)
union
SELECT n2t2.AppObjectTypeCode as Type,
n1t1.AppObjectCode as Code,
n0t0.NetworkAlias as NetworkUser,
n0t0.TimeStamp as TimeStamp,
convert(int,replace(n0t0.Details, 'Rows Returned = ','')) as Details,
n0t0.Server as Server
FROM NxMetricViewerLog AS n0t0 WITH ( nolock ) 
JOIN NxFWKAppObject AS n1t1 WITH ( nolock ) 
JOIN NxFWKAppObjectType AS n2t2 WITH ( nolock ) 
ON ( n1t1.AppObjectTypeId ) = ( n2t2.NxFWKAppObjectTypeID ) 
ON ( n0t0.AppObjectID ) = ( n1t1.NxFWKAppObjectID ) 
WHERE convert(int,replace(n0t0.Details, 'Rows Returned = ','')) > @rowcnt
and n0t0.TimeStamp >= convert(datetime,@starttime,20)
and n0t0.TimeStamp <= convert(datetime,@endtime,20)
order by details desc
GO

create procedure [dbo].[sp_3ehealthmonitor] 
@ServerName varchar(50),
@starttime smalldatetime,
@endtime smalldatetime,
@ApplicationEvent int, 
@MetricRuns int,
@MetricViews int,
@Reports int,
@PerformanceLogs int,
@Printing int,
@ProcessSteps int,
@ProcessTasks int

as

-- CHECK TABLE EXISTENCE

-- Check to see if the Temporary Table exists. If it does, drop it, else create the Temporary Table

IF OBJECT_ID('tempdb..#3ETempTable') IS NOT NULL 

      BEGIN

            Drop table #3ETempTable 

      END

      ELSE BEGIN

            CREATE TABLE #3ETempTable (

            Type text,

            Code text,

            NetworkUser text,

            TimeStamp SmallDateTime,

            Details text,

			Server nvarchar(128)

            )

      END
      
-- REQUIRED FIELDS AND OPTIONS
-- Server Name, Start Time and End Time are Required. The variables that follow can be excluded from the query. By default, 

-- they are included as noted by the  = 1. To exclude the sections that you want, replace the 1 with a 0. 


--Application Events

If @ApplicationEvent = 1

      BEGIN

            insert into #3ETempTable

 

            SELECT  'Application Event' as Type,

            n0t0.EventName as Code,

            n0t0.NetworkAlias as NetworkUser,

            n0t0.LogTime as TimeStamp,

            n0t0.Details as Details,
			n0t0.Server as Server

            FROM NxAppEvent AS n0t0 

            where n0t0.Server  like  N'%' + @servername + '%' 

            and n0t0.LogTime >= convert(datetime,@starttime,20)

            and n0t0.LogTime <= convert(datetime,@endtime,20)

      END

 

--Metric Runs

If @MetricRuns = 1

      BEGIN

            insert into #3ETempTable

 

            SELECT 'Metric Run' as Type,

            n2t1.AppObjectCode as Code,

            n0t0.NetworkAlias as NetworkUser,

            n0t0.StartTime as TimeStamp,

            cast(n0t0.IsRunComplete as varchar) as Details,
			n0t0.MachineName  as Server

            FROM NxMetricRun AS n0t0 WITH ( nolock ) 

            JOIN NxMetric AS n1t2 WITH ( nolock ) 

            LEFT OUTER JOIN NxFWKAppObject AS n2t1 WITH ( nolock ) 

            ON ( n1t2.MetricArchetypeID ) = ( n2t1.NxFWKAppObjectID ) 

            ON ( n0t0.MetricID ) = ( n1t2.NxMetricID ) 

            WHERE n0t0.MachineName  like  N'%' + @servername + '%' 

            and n0t0.StartTime >= convert(datetime,@starttime,20)

            and n0t0.StartTime <= convert(datetime,@endtime,20)

      END

 

--Metric Views

If @MetricViews = 1

      BEGIN

            insert into #3ETempTable

            SELECT n2t2.AppObjectTypeCode as Type,

            n1t1.AppObjectCode as Code,

            n0t0.NetworkAlias as NetworkUser,

            n0t0.TimeStamp as TimeStamp,

            n0t0.Details as Details,
			n0t0.Server as Server

            FROM NxMetricViewerLog AS n0t0 WITH ( nolock ) 

            JOIN NxFWKAppObject AS n1t1 WITH ( nolock ) 

            JOIN NxFWKAppObjectType AS n2t2 WITH ( nolock ) 

            ON ( n1t1.AppObjectTypeId ) = ( n2t2.NxFWKAppObjectTypeID ) 

            ON ( n0t0.AppObjectID ) = ( n1t1.NxFWKAppObjectID ) 

            WHERE n0t0.Server  like  N'%' + @servername + '%' 

            and n0t0.TimeStamp >= convert(datetime,@starttime,20)

            and n0t0.TimeStamp <= convert(datetime,@endtime,20)

      END

--Reports

If @Reports = 1

      BEGIN

            insert into #3ETempTable

            SELECT 'Reports' as Type,

            n2t2.AppObjectTypeCode + ' - ' + n1t1.AppObjectCode as Code, 

            n0t0.NetworkAlias as NetworkUser,

            n0t0.TimeStamp as Timestamp, 

            n0t0.Details as Details,
			n0t0.Server as Server

            FROM NxReportLog AS n0t0 WITH ( nolock ) 

            JOIN NxFWKAppObject AS n1t1 WITH ( nolock ) 

            JOIN NxFWKAppObjectType AS n2t2 WITH ( nolock ) 

            ON ( n1t1.AppObjectTypeId ) = ( n2t2.NxFWKAppObjectTypeID ) 

            ON ( n0t0.AppObjectID ) = ( n1t1.NxFWKAppObjectID ) 

            WHERE n0t0.Server  like  N'%' + @servername + '%' 

            and n0t0.TimeStamp >= convert(datetime,@starttime,20)

            and n0t0.TimeStamp <= convert(datetime,@endtime,20)

      END

--Performance Logs

If @PerformanceLogs = 1

      BEGIN

            insert into #3ETempTable

            SELECT 'Performance Counters' as Type,

            n1t1.CounterCategory + ' - ' + n1t1.Code as Code,

            n0t0.NetworkAlias as NetworkUser,

            n0t0.LogTime as Timestamp,

            cast(n0t0.PerformanceCounterValue as varchar) + ' ' + n1t1.CounterFormat as Details,

			n0t0.Server as Server

            FROM NxPerfMon AS n0t0 WITH ( nolock ) 

            JOIN NxPerfMonCounterList AS n1t1 WITH ( nolock ) 

            ON ( n0t0.PerformanceCounter ) = ( n1t1.NxPerfMonCounterListID ) 

            where n0t0.Server  like  N'%' + @servername + '%' 

            and n0t0.LogTime >= convert(datetime,@starttime,20)

            and n0t0.LogTime <= convert(datetime,@endtime,20)

      END

 

--Printing

If @Printing = 1

      BEGIN

            insert into #3ETempTable

 

            SELECT 'Printing' as Type,

            n0t0.Name as Code,

            n0t0.NetworkAlias as NetworkUser,

            n0t0.StartTime as Timestamp,

            n0t0.Status as Details,
			n0t0.MachineName as Server

            FROM NxPrintJob AS n0t0 WITH ( nolock ) 

            WHERE n0t0.MachineName  like  N'%' + @servername + '%' 

            and n0t0.StartTime >= convert(datetime,@starttime,20)

            and n0t0.StartTime <= convert(datetime,@endtime,20)

      END

--Process Steps

If @ProcessSteps = 1

      BEGIN

            insert into #3ETempTable

            SELECT 'Process Steps' as Type,

            n1t1.ProcCode as 'Code',

            n2t2.BaseUserName as NetworkUser,

            MAX(n0t0.LastAccessTime ) as Timestamp,

            'N/A' as Details,
			n0t0.ServerName as Server

            FROM NxFWKProcessItemStep AS n0t0 WITH ( nolock ) 

            JOIN NxFWKProcessItem AS n1t1 WITH ( nolock ) 

            JOIN NxBaseUser AS n2t2 WITH ( nolock ) 

            ON ( n1t1.UserId ) = ( n2t2.NxBaseUserID ) 

            ON ( n0t0.ProcItemID ) = ( n1t1.ProcItemID ) 

            WHERE ( n0t0.ServerName ) like  N'%' + @servername + '%' 

            and n0t0.LastAccessTime >= convert(datetime,@starttime,20)

            and n0t0.LastAccessTime <= convert(datetime,@endtime,20)

            GROUP BY n1t1.ProcCode,n2t2.BaseUserName, n0t0.ServerName

      END

 

--Process/Tasks Queue

If @ProcessTasks = 1

      BEGIN

            insert into #3ETempTable

            

            SELECT 'Process/Tasks' as Type,

            n2t2.Name as Code,

            'Cobra Admin' as NetworkUser,

            n0t0.QueueEntryTime as TimeStamp, 

            n0t0.Log as Details ,

		n0t0.Server as Server

            FROM NxNtfTaskQueue AS n0t0 

            LEFT OUTER JOIN NxNtfServer AS n1t1 

            ON ( n0t0.Server ) = ( n1t1.NxNtfServerID ) 

            LEFT OUTER JOIN NxNtfTask AS n2t2 

            LEFT OUTER JOIN NxBaseUser AS n3t3 

            ON ( n2t2.RunAsUser ) = ( n3t3.NxBaseUserID ) 

            ON ( n0t0.Task ) = ( n2t2.NxNtfTaskID ) 

            WHERE ( n1t1.ServerName ) like  N'%' + @servername + '%' 

            and n0t0.QueueEntryTime >= convert(datetime,@starttime,20)

            and n0t0.QueueEntryTime <= convert(datetime,@endtime,20)

      END

-- Select records to view in Temporary Table

select * from #3ETempTable 

order by timestamp desc


--Drop Temporary Table

Drop table #3ETempTable


*/
-- END creation of stored procedures

--Usage1: 				
--sp_3ehealthmonitor 'UK-3E-SUP02','2009-08-25 09:00','2009-08-25 11:00',1,1,1,1,1,1,1,1

--Usage2 (reports and cubes only)
--sp_3ehealthmonitor 'UK-3E-SUP02','2009-08-25 09:00','2009-08-25 11:00',0,1,1,1,0,0,0,0

--Usage3 (reports and cubes only ALL servers)
--sp_3ehealthmonitor '%','2009-08-25 09:00','2009-08-25 11:00',0,1,1,1,0,0,0,0

--Usage4: All Servers All Events
--sp_3ehealthmonitor '%','2009-08-25 09:00','2009-08-25 11:00',1,1,1,1,1,1,1,1

-- Large Reports/Metrics being run
DECLARE @v_startdate as datetime
DECLARE @v_enddate as datetime
SET @v_startdate = getdate()-2
SET @v_enddate = getdate()
exec sp_3ehealthmonitorbigreports @v_startdate, @v_enddate, 999


-- 3E PRINT JOBS

-- Shows top FAILED print jobs
select top 20 * from nxprintjob
where resultcode = -1
order by starttime desc

--Shows top RUNNING/HUNG print jobs
select top 20 * from nxprintjob
where resultcode = 2
order by starttime desc

--Shows all recent print jobs
select top 20 * from nxprintjob
order by starttime desc

-- Stock appobjects that have been customised

SELECT ao.NxFWKAppObjectID,ao.AppObjectCode,aot.AppObjectTypeCode,
LTRIM(STR(majorversion)) + '.' + LTRIM(STR(minorversion)) + '.' + LTRIM(STR(build)) + '.' + LTRIM(STR(revision)) AS Version, 'Custom' AS Type
INTO #tmp_customversions
FROM [NxFWKAppObject] ao 
JOIN NxFWKAppObjectType aot on aot.NxFWKAppObjectTypeId = ao.AppObjectTypeId 
JOIN NxFWKAppObjectData aod on ao.NxFWKAppObjectId = aod.AppObjectId 
WHERE aot.AppObjectTypeCode IN ('Process' , 'Project' , 'Form' ,'BusinessObject' , 'Archetype', 'Object' ,'Page')
AND aod.IsCustom='1' 
AND ao.NxFWKAppObjectID in (SELECT ao.NxFWKAppObjectID FROM [NxFWKAppObject] ao 
JOIN NxFWKAppObjectType aot on aot.NxFWKAppObjectTypeId = ao.AppObjectTypeId 
JOIN NxFWKAppObjectData aod on ao.NxFWKAppObjectId = aod.AppObjectId 
WHERE aot.AppObjectTypeCode IN ( 'Process' , 'Project' , 'Form' ,'BusinessObject' , 'Archetype', 'Object' ,'Page')
AND aod.IsCustom='0')
ORDER By ao.AppObjectCode


SELECT ao.NxFWKAppObjectID,ao.AppObjectCode,aot.AppObjectTypeCode,
LTRIM(STR(majorversion)) + '.' + LTRIM(STR(minorversion)) + '.' + LTRIM(STR(build)) + '.' + LTRIM(STR(revision)) AS Version, 'Stock' AS Type
INTO #tmp_stockversions
FROM [NxFWKAppObject] ao 
JOIN NxFWKAppObjectType aot on aot.NxFWKAppObjectTypeId = ao.AppObjectTypeId 
JOIN NxFWKAppObjectData aod on ao.NxFWKAppObjectId = aod.AppObjectId 
WHERE aot.AppObjectTypeCode IN ( 'Process' , 'Project' , 'Form' ,'BusinessObject' , 'Archetype', 'Object' ,'Page')
AND aod.IsCustom='0' 
AND ao.NxFWKAppObjectID in (SELECT NxFWKAppObjectID FROM #tmp_customversions)
ORDER By ao.AppObjectCode

SELECT c.appobjectcode as AppObjectName, c.AppObjectTypeCode as Type, s.version as StockVersion, c.version as CustomVersion
FROM #tmp_stockversions s, #tmp_customversions c
WHERE s.NxFWKAppObjectID = c.NxFWKAppObjectID

drop table #tmp_customversions
drop table #tmp_stockversions

-- Recent IDE changes

SELECT TOP 100 appobjecttypecode AS Type, appobjectcode As Name,
       LTRIM(STR(majorversion)) + '.' + LTRIM(STR(minorversion)) + '.' + LTRIM(STR(build)) + '.' + LTRIM(STR(revision)) AS Version, 
       savedby, savedate, iscustom
FROM NxFWKAppObjectDataHistory
ORDER BY savedate DESC

-- Customized Archetypes

select so.name 'Table', sc.name 'Column', 1 'IsCustom'
into #tmp_3earchetypes
from sysobjects so
JOIN  syscolumns sc on so.id = sc.id
where so.name in (
	SELECT ao.AppObjectCode
	FROM [NxFWKAppObject] ao 
	JOIN NxFWKAppObjectType aot on aot.NxFWKAppObjectTypeId = ao.AppObjectTypeId 
	JOIN NxFWKAppObjectData aod on ao.NxFWKAppObjectId = aod.AppObjectId 
	WHERE aot.AppObjectTypeCode IN ( 'Archetype')
	AND aod.IsCustom='1' 
)
and sc.name not in ('templateid', 'itemid', 'archetypecode', 'currprocitemid', 'lastprocitemid', 'origprocitemid', 'hasattachments', 'timestamp')


insert into #tmp_3earchetypes
select replace(so.name,'_Template',''), sc.name, 0 'IsCustom'
from sysobjects so
JOIN  syscolumns sc on so.id = sc.id
where so.name in (
	SELECT ao.AppObjectCode + '_template'
	FROM [NxFWKAppObject] ao 
	JOIN NxFWKAppObjectType aot on aot.NxFWKAppObjectTypeId = ao.AppObjectTypeId 
	JOIN NxFWKAppObjectData aod on ao.NxFWKAppObjectId = aod.AppObjectId 
	WHERE aot.AppObjectTypeCode IN ( 'Archetype')
	AND aod.IsCustom='1' 
)
and sc.name not in ('templateid', 'itemid', 'archetypecode', 'currprocitemid', 'lastprocitemid', 'origprocitemid', 'hasattachments', 'timestamp')

select * from #tmp_3earchetypes
where iscustom=1
and [Table] + '_' + [column] not in ( select [Table] + '_' + [column] from #tmp_3earchetypes where iscustom = 0)

drop table #tmp_3earchetypes


-- Metric Count
select cast(NMRUN.description as varchar(500)), count(*) as MetricCount, NMRUN.ArchetypeCode,
min(NMRUN.timestamp) as OldestTimeStamp, max(NMRUN.timestamp) as NewestTimeStamp,
min(NMRUN.runindex) as OldestRun#, max(NMRUN.runindex) as NewestRun#
from nxmetricrun as NMRUN
group by cast(NMRUN.description as varchar(500)), NMRUN.ArchetypeCode
order by count(*) desc

-- Metrics by size

If Object_ID('dbo.NxFWKAppObject') is not null 
SELECT NXM.Description, AO.appobjectcode + CAST(NXMR.RunIndex as nvarchar) AS Table_Name,
CAST(SUM(DMPS.reserved_page_count * 8) AS nvarchar)+' KB' AS size_in_KB
FROM dbo.NxFWKAppObject AO
LEFT JOIN dbo.NxFWKAppObjectData AOD on AO.NxFWKAppObjectID = AOD.AppObjectID
LEFT JOIN dbo.NxFWKAppObjectType AOT on AO.AppObjectTypeID = AOT.NxFWKAppObjectTypeID
LEFT JOIN dbo.NxMetric NXM on NXM.MetricArchetypeID = AOD.AppObjectId
LEFT JOIN dbo.NxMetricRun NXMR on NXMR.MetricID = NXM.NxMetricId
LEFT JOIN sys.objects SO on SO.name = AO.appobjectcode + CAST(NXMR.RunIndex as nvarchar)
LEFT JOIN sys.dm_db_partition_stats DMPS on object_name(DMPS.object_id) = AO.appobjectcode + CAST (NXMR.RunIndex as nvarchar)
WHERE AOT.appobjecttypecode = 'MetricArchetype' and NXMR.RunIndex is not NULL
GROUP BY NXM.Description, AO.appobjectcode + CAST(NXMR.RunIndex as nvarchar)
ORDER by NXM.Description

-- Display process count by wapi

select NxFWKProcessItemStep.ServerName, count(*) 'ProcessSteps'
from nxfwkprocessitem 
		inner join nxfwkprocessitemstep on nxfwkprocessitem.ProcItemID = NxFWKProcessItemStep.ProcItemID
where NxFWKProcessItemStep.CreateDateTime between '2021-03-24 02:00:00' and '2021-03-24 07:00:00'
group by NxFWKProcessItemStep.ServerName
--Display process count by wapi and user

select NxFWKProcessItemStep.ServerName, NxFWKUser.NetworkAlias, count(*) 'ProcessSteps'
from nxfwkprocessitem 
		inner join nxfwkprocessitemstep on nxfwkprocessitem.ProcItemID = NxFWKProcessItemStep.ProcItemID
		inner join nxfwkuser on nxfwkprocessitem.UserId = NxFWKUser.NxFWKUserID
group by NxFWKProcessItemStep.ServerName, NxFWKUser.NetworkAlias


-- If needed view activity by WAPI server for a specific date range
select nxfwkprocessitem.ProcCode,nxfwkprocessitem.name, count(*) as ExecCount
from nxfwkprocessitem 
		inner join nxfwkprocessitemstep on nxfwkprocessitem.ProcItemID = NxFWKProcessItemStep.ProcItemID
where ServerName = 'YourWAPIServer'
and NxFWKProcessItemStep.CreateDateTime between '2019-06-07 06:00:00' and '2019-06-07 10:00:00'
group by nxfwkprocessitem.ProcCode,nxfwkprocessitem.name

-- 3E SPECIFICS END
