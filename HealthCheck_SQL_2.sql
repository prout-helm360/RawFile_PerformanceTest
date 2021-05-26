-- Bimal Dave
-- Collection of scripts to perform a SQL Server Health Check 

-- SQL Server Version, Memory and Processor
exec master.dbo.xp_msver

-- DB Info
sp_helpdb 

-- Instance config info

SELECT name, value, value_in_use, minimum, maximum, [description], is_dynamic, is_advanced
FROM sys.configurations WITH (NOLOCK)
ORDER BY name OPTION (RECOMPILE);

-- SQL Server last restarted datetime

	SELECT 
		CAST(create_date AS VARCHAR(100)) 'SQL Restart Date'
	FROM    sys.databases
	WHERE   database_id = 2

-- List all global trace flags that are enabled

DBCC TRACESTATUS (-1);

-- Database File Config
EXEC sp_msforeachdb 'select * from [?].sys.database_files'

-- Physical Disk Space

EXEC MASTER..xp_fixeddrives

-- Transaction Log Space

dbcc sqlperf('logspace')

-- SQL Server ErrorLog
sp_readerrorlog

-- Current queries by CPU
select der.session_id, der.start_time, der.status, der.command, database_id, 
blocking_session_id,wait_type, wait_time, wait_resource, last_wait_type, 
cpu_time, total_elapsed_time, logical_reads, query_hash, text
from  sys.dm_exec_requests as der
             cross apply sys.dm_exec_sql_text (der.sql_handle) as dest
order by cpu_time desc

--Server WAITS 
WITH [Waits] AS
    (SELECT
        [wait_type],
        [wait_time_ms] / 1000.0 AS [WaitS],
        ([wait_time_ms] - [signal_wait_time_ms]) / 1000.0 AS [ResourceS],
        [signal_wait_time_ms] / 1000.0 AS [SignalS],
        [waiting_tasks_count] AS [WaitCount],
        100.0 * [wait_time_ms] / SUM ([wait_time_ms]) OVER() AS [Percentage],
        ROW_NUMBER() OVER(ORDER BY [wait_time_ms] DESC) AS [RowNum]
    FROM sys.dm_os_wait_stats
    WHERE [wait_type] NOT IN (
        -- These wait types are almost 100% never a problem and so they are
        -- filtered out to avoid them skewing the results. Click on the URL
        -- for more information.
        N'BROKER_EVENTHANDLER', -- https://www.sqlskills.com/help/waits/BROKER_EVENTHANDLER
        N'BROKER_RECEIVE_WAITFOR', -- https://www.sqlskills.com/help/waits/BROKER_RECEIVE_WAITFOR
        N'BROKER_TASK_STOP', -- https://www.sqlskills.com/help/waits/BROKER_TASK_STOP
        N'BROKER_TO_FLUSH', -- https://www.sqlskills.com/help/waits/BROKER_TO_FLUSH
        N'BROKER_TRANSMITTER', -- https://www.sqlskills.com/help/waits/BROKER_TRANSMITTER
        N'CHECKPOINT_QUEUE', -- https://www.sqlskills.com/help/waits/CHECKPOINT_QUEUE
        N'CHKPT', -- https://www.sqlskills.com/help/waits/CHKPT
        N'CLR_AUTO_EVENT', -- https://www.sqlskills.com/help/waits/CLR_AUTO_EVENT
        N'CLR_MANUAL_EVENT', -- https://www.sqlskills.com/help/waits/CLR_MANUAL_EVENT
        N'CLR_SEMAPHORE', -- https://www.sqlskills.com/help/waits/CLR_SEMAPHORE
        N'CXCONSUMER', -- https://www.sqlskills.com/help/waits/CXCONSUMER
 
        -- Maybe comment these four out if you have mirroring issues
        N'DBMIRROR_DBM_EVENT', -- https://www.sqlskills.com/help/waits/DBMIRROR_DBM_EVENT
        N'DBMIRROR_EVENTS_QUEUE', -- https://www.sqlskills.com/help/waits/DBMIRROR_EVENTS_QUEUE
        N'DBMIRROR_WORKER_QUEUE', -- https://www.sqlskills.com/help/waits/DBMIRROR_WORKER_QUEUE
        N'DBMIRRORING_CMD', -- https://www.sqlskills.com/help/waits/DBMIRRORING_CMD
 
        N'DIRTY_PAGE_POLL', -- https://www.sqlskills.com/help/waits/DIRTY_PAGE_POLL
        N'DISPATCHER_QUEUE_SEMAPHORE', -- https://www.sqlskills.com/help/waits/DISPATCHER_QUEUE_SEMAPHORE
        N'EXECSYNC', -- https://www.sqlskills.com/help/waits/EXECSYNC
        N'FSAGENT', -- https://www.sqlskills.com/help/waits/FSAGENT
        N'FT_IFTS_SCHEDULER_IDLE_WAIT', -- https://www.sqlskills.com/help/waits/FT_IFTS_SCHEDULER_IDLE_WAIT
        N'FT_IFTSHC_MUTEX', -- https://www.sqlskills.com/help/waits/FT_IFTSHC_MUTEX
 
        -- Maybe comment these six out if you have AG issues
        N'HADR_CLUSAPI_CALL', -- https://www.sqlskills.com/help/waits/HADR_CLUSAPI_CALL
        N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', -- https://www.sqlskills.com/help/waits/HADR_FILESTREAM_IOMGR_IOCOMPLETION
        N'HADR_LOGCAPTURE_WAIT', -- https://www.sqlskills.com/help/waits/HADR_LOGCAPTURE_WAIT
        N'HADR_NOTIFICATION_DEQUEUE', -- https://www.sqlskills.com/help/waits/HADR_NOTIFICATION_DEQUEUE
        N'HADR_TIMER_TASK', -- https://www.sqlskills.com/help/waits/HADR_TIMER_TASK
        N'HADR_WORK_QUEUE', -- https://www.sqlskills.com/help/waits/HADR_WORK_QUEUE
 
        N'KSOURCE_WAKEUP', -- https://www.sqlskills.com/help/waits/KSOURCE_WAKEUP
        N'LAZYWRITER_SLEEP', -- https://www.sqlskills.com/help/waits/LAZYWRITER_SLEEP
        N'LOGMGR_QUEUE', -- https://www.sqlskills.com/help/waits/LOGMGR_QUEUE
        N'MEMORY_ALLOCATION_EXT', -- https://www.sqlskills.com/help/waits/MEMORY_ALLOCATION_EXT
        N'ONDEMAND_TASK_QUEUE', -- https://www.sqlskills.com/help/waits/ONDEMAND_TASK_QUEUE
        N'PARALLEL_REDO_DRAIN_WORKER', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_DRAIN_WORKER
        N'PARALLEL_REDO_LOG_CACHE', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_LOG_CACHE
        N'PARALLEL_REDO_TRAN_LIST', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_TRAN_LIST
        N'PARALLEL_REDO_WORKER_SYNC', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_WORKER_SYNC
        N'PARALLEL_REDO_WORKER_WAIT_WORK', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_WORKER_WAIT_WORK
        N'PREEMPTIVE_XE_GETTARGETSTATE', -- https://www.sqlskills.com/help/waits/PREEMPTIVE_XE_GETTARGETSTATE
        N'PWAIT_ALL_COMPONENTS_INITIALIZED', -- https://www.sqlskills.com/help/waits/PWAIT_ALL_COMPONENTS_INITIALIZED
        N'PWAIT_DIRECTLOGCONSUMER_GETNEXT', -- https://www.sqlskills.com/help/waits/PWAIT_DIRECTLOGCONSUMER_GETNEXT
        N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', -- https://www.sqlskills.com/help/waits/QDS_PERSIST_TASK_MAIN_LOOP_SLEEP
        N'QDS_ASYNC_QUEUE', -- https://www.sqlskills.com/help/waits/QDS_ASYNC_QUEUE
        N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
            -- https://www.sqlskills.com/help/waits/QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP
        N'QDS_SHUTDOWN_QUEUE', -- https://www.sqlskills.com/help/waits/QDS_SHUTDOWN_QUEUE
        N'REDO_THREAD_PENDING_WORK', -- https://www.sqlskills.com/help/waits/REDO_THREAD_PENDING_WORK
        N'REQUEST_FOR_DEADLOCK_SEARCH', -- https://www.sqlskills.com/help/waits/REQUEST_FOR_DEADLOCK_SEARCH
        N'RESOURCE_QUEUE', -- https://www.sqlskills.com/help/waits/RESOURCE_QUEUE
        N'SERVER_IDLE_CHECK', -- https://www.sqlskills.com/help/waits/SERVER_IDLE_CHECK
        N'SLEEP_BPOOL_FLUSH', -- https://www.sqlskills.com/help/waits/SLEEP_BPOOL_FLUSH
        N'SLEEP_DBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_DBSTARTUP
        N'SLEEP_DCOMSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_DCOMSTARTUP
        N'SLEEP_MASTERDBREADY', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERDBREADY
        N'SLEEP_MASTERMDREADY', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERMDREADY
        N'SLEEP_MASTERUPGRADED', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERUPGRADED
        N'SLEEP_MSDBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_MSDBSTARTUP
        N'SLEEP_SYSTEMTASK', -- https://www.sqlskills.com/help/waits/SLEEP_SYSTEMTASK
        N'SLEEP_TASK', -- https://www.sqlskills.com/help/waits/SLEEP_TASK
        N'SLEEP_TEMPDBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_TEMPDBSTARTUP
        N'SNI_HTTP_ACCEPT', -- https://www.sqlskills.com/help/waits/SNI_HTTP_ACCEPT
        N'SOS_WORK_DISPATCHER', -- https://www.sqlskills.com/help/waits/SOS_WORK_DISPATCHER
        N'SP_SERVER_DIAGNOSTICS_SLEEP', -- https://www.sqlskills.com/help/waits/SP_SERVER_DIAGNOSTICS_SLEEP
        N'SQLTRACE_BUFFER_FLUSH', -- https://www.sqlskills.com/help/waits/SQLTRACE_BUFFER_FLUSH
        N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', -- https://www.sqlskills.com/help/waits/SQLTRACE_INCREMENTAL_FLUSH_SLEEP
        N'SQLTRACE_WAIT_ENTRIES', -- https://www.sqlskills.com/help/waits/SQLTRACE_WAIT_ENTRIES
        N'WAIT_FOR_RESULTS', -- https://www.sqlskills.com/help/waits/WAIT_FOR_RESULTS
        N'WAITFOR', -- https://www.sqlskills.com/help/waits/WAITFOR
        N'WAITFOR_TASKSHUTDOWN', -- https://www.sqlskills.com/help/waits/WAITFOR_TASKSHUTDOWN
        N'WAIT_XTP_RECOVERY', -- https://www.sqlskills.com/help/waits/WAIT_XTP_RECOVERY
        N'WAIT_XTP_HOST_WAIT', -- https://www.sqlskills.com/help/waits/WAIT_XTP_HOST_WAIT
        N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', -- https://www.sqlskills.com/help/waits/WAIT_XTP_OFFLINE_CKPT_NEW_LOG
        N'WAIT_XTP_CKPT_CLOSE', -- https://www.sqlskills.com/help/waits/WAIT_XTP_CKPT_CLOSE
        N'XE_DISPATCHER_JOIN', -- https://www.sqlskills.com/help/waits/XE_DISPATCHER_JOIN
        N'XE_DISPATCHER_WAIT', -- https://www.sqlskills.com/help/waits/XE_DISPATCHER_WAIT
        N'XE_TIMER_EVENT' -- https://www.sqlskills.com/help/waits/XE_TIMER_EVENT
        )
    AND [waiting_tasks_count] > 0
    )
SELECT
    MAX ([W1].[wait_type]) AS [WaitType],
    CAST (MAX ([W1].[WaitS]) AS DECIMAL (16,2)) AS [Wait_S],
    CAST (MAX ([W1].[ResourceS]) AS DECIMAL (16,2)) AS [Resource_S],
    CAST (MAX ([W1].[SignalS]) AS DECIMAL (16,2)) AS [Signal_S],
    MAX ([W1].[WaitCount]) AS [WaitCount],
    CAST (MAX ([W1].[Percentage]) AS DECIMAL (5,2)) AS [Percentage],
    CAST ((MAX ([W1].[WaitS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgWait_S],
    CAST ((MAX ([W1].[ResourceS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgRes_S],
    CAST ((MAX ([W1].[SignalS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgSig_S],
    CAST ('https://www.sqlskills.com/help/waits/' + MAX ([W1].[wait_type]) as XML) AS [Help/Info URL]
FROM [Waits] AS [W1]
INNER JOIN [Waits] AS [W2] ON [W2].[RowNum] <= [W1].[RowNum]
GROUP BY [W1].[RowNum]
HAVING SUM ([W2].[Percentage]) - MAX( [W1].[Percentage] ) < 95; -- percentage threshold
GO

--Top 10 queries by IO
SELECT TOP 10 
 [Average IO] = (total_logical_reads + total_logical_writes) / qs.execution_count
,[Total IO] = (total_logical_reads + total_logical_writes)
,[Execution count] = qs.execution_count
,[Individual Query] = SUBSTRING (qt.text,qs.statement_start_offset/2, 
         (CASE WHEN qs.statement_end_offset = -1 
            THEN LEN(CONVERT(NVARCHAR(MAX), qt.text)) * 2 
          ELSE qs.statement_end_offset END - qs.statement_start_offset)/2) 
        ,[Parent Query] = qt.text
,DatabaseName = DB_NAME(qt.dbid)
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) as qt
ORDER BY [Average IO] DESC;

--Database read/writes
SELECT TOP 10 
        [Total Reads] = SUM(total_logical_reads)
        ,[Execution count] = SUM(qs.execution_count)
        ,DatabaseName = DB_NAME(qt.dbid)
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) as qt
GROUP BY DB_NAME(qt.dbid)
ORDER BY [Total Reads] DESC;

SELECT TOP 10 
        [Total Writes] = SUM(total_logical_writes)
        ,[Execution count] = SUM(qs.execution_count)
        ,DatabaseName = DB_NAME(qt.dbid)
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) as qt
GROUP BY DB_NAME(qt.dbid)
ORDER BY [Total Writes] DESC;

--Missing indexes by database
SELECT 
    DatabaseName = DB_NAME(database_id)
    ,[Number Indexes Missing] = count(*) 
FROM sys.dm_db_missing_index_details
GROUP BY DB_NAME(database_id)
ORDER BY 2 DESC;

--Costly Missing Indexes
SELECT  TOP 10 
        [Total Cost]  = ROUND(avg_total_user_cost * avg_user_impact * (user_seeks + user_scans),0) 
        , avg_user_impact
        , TableName = statement
        , [EqualityUsage] = equality_columns 
        , [InequalityUsage] = inequality_columns
        , [Include Cloumns] = included_columns
FROM        sys.dm_db_missing_index_groups g 
INNER JOIN    sys.dm_db_missing_index_group_stats s 
       ON s.group_handle = g.index_group_handle 
INNER JOIN    sys.dm_db_missing_index_details d 
       ON d.index_handle = g.index_handle
ORDER BY [Total Cost] DESC;

-- Use the following to generate an index creation/drop script based on costly missing indexes above
/*
USE master
GO

CREATE FUNCTION [dbo].[fn_Index_CreateIndexName] (@equality_columns NVARCHAR(4000), 
@Inequality_columns NVARCHAR(4000), @index_handlE INT) RETURNS VARCHAR(128)
AS
BEGIN
	
	DECLARE @IndexName NVARCHAR(255)

	SET @IndexName = ISNULL(@equality_columns,@Inequality_columns)

	SET @IndexName = LTRIM(REPLACE(@IndexName,'[','_'))

	SET @IndexName = RTRIM(REPLACE(@IndexName,']','_'))

	SET @IndexName = REPLACE(@IndexName,',','')

	SET @IndexName = REPLACE(@IndexName,'_ _','_')

	IF LEN(@IndexName) > 120
	BEGIN

		SET @IndexName = SUBSTRING(@IndexName,0,120)

	END  

	SET @IndexName = @IndexName + CAST(@index_handlE AS NVARCHAR(15))
	 
	RETURN @IndexName 
END

GO 

CREATE VIEW [dbo].[vw_Index_MissingIndex]
AS

SELECT  '[' + d.name + ']' as DBName,
        [dbo].[fn_Index_CreateIndexName]
        (mid.equality_columns,mid.Inequality_columns,mid.index_handlE) AS ID,
        REPLACE(mid.equality_columns,',',' ASC,') AS equality_columns,
        REPLACE(mid.Inequality_columns,',',' ASC,') AS Inequality_columns,
        mid.Included_columns,
        mid.[statement]
FROM sys.dm_db_missing_index_details mid
INNER JOIN sys.databases d
on d.database_id = mid.database_id
GO

CREATE PROCEDURE [dbo].[usp_Index_MissingIndexCreationStatements]
AS

DECLARE @IndexCreationPlaceholder_Start  AS NVARCHAR(MAX)
DECLARE @IndexCreationPlaceholder_End  AS NVARCHAR(MAX)

-- PREPARE PLACEHOLDER

SET @IndexCreationPlaceholder_Start = 'IF NOT EXISTS(SELECT * FROM {2}.sys.indexes WHERE [name] = ''IX_{0}'' )
				BEGIN
				CREATE NONCLUSTERED INDEX [IX_{0}] ON {1}'

SET @IndexCreationPlaceholder_End = ' WITH (PAD_INDEX = OFF, _
STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, 
ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
				END;' + char(13) + char(10)

-- STATEMENT CREATION
 
SELECT
	DBName,
	CASE
	WHEN NOT mid.equality_columns IS NULL AND NOT mid.Inequality_columns IS NULL THEN
				REPLACE(REPLACE(REPLACE(@IndexCreationPlaceholder_Start,'{0}', 
				mid.ID),'{1}',mid.[statement]),'{2}',mid.DBName)
				+ '
				   ( ' +
				   COALESCE(mid.equality_columns,'') +
				   ' ASC,' + 
				   COALESCE(mid.Inequality_columns,'') +
				   ' ASC
				)' +
				COALESCE('INCLUDE ( ' + mid.Included_columns + ' ) ','')
				+ @IndexCreationPlaceholder_End

		WHEN mid.equality_columns IS NULL AND NOT mid.Inequality_columns IS NULL THEN
				REPLACE(REPLACE(REPLACE(@IndexCreationPlaceholder_Start,
				'{0}', mid.ID),'{1}',mid.[statement]),'{2}',mid.DBName)
				+ '
				   ( ' +
				   COALESCE(mid.Inequality_columns,'') +
				   ' ASC
				) ' +
				COALESCE('INCLUDE ( ' + mid.Included_columns + ' ) ','')
				+ @IndexCreationPlaceholder_End
				
		WHEN NOT mid.equality_columns IS NULL AND mid.Inequality_columns IS NULL THEN
				REPLACE(REPLACE(REPLACE(@IndexCreationPlaceholder_Start,'{0}', 
				mid.ID),'{1}',mid.[statement]),'{2}',mid.DBName)
				+ '
				   ( ' +
			   COALESCE(mid.equality_columns,'') +  ' ASC
					) ' +
				COALESCE('INCLUDE ( ' + mid.Included_columns + ' ) ','')
				+ @IndexCreationPlaceholder_End
		ELSE NULL
	END AS Index_Creation_Statement,
	' DROP INDEX [IX_' + mid.ID  + '] ON ' + mid.[statement]  
	+  + char(13) + char(10) AS Index_Drop_Statement
FROM [dbo].[vw_Index_MissingIndex] AS mid

EXEC usp_Index_MissingIndexCreationStatements
*/


--Fragmented indexes (focus on page count > 1000)

SELECT 
    OBJECT_NAME(A.[object_id]) as 'TableName', 
    B.[name] as 'IndexName', 
    A.[index_id], 
    A.[page_count], 
    A.[index_type_desc], 
    A.[avg_fragmentation_in_percent], 
    A.[fragment_count] 
FROM 
    sys.dm_db_index_physical_stats(db_id(),NULL,NULL,NULL,'LIMITED') A INNER JOIN 
    sys.indexes B ON A.[object_id] = B.[object_id] and A.index_id = B.index_id  
WHERE A.[avg_fragmentation_in_percent] >0
ORDER BY A.page_count DESC, A.[avg_fragmentation_in_percent] DESC

--Get most used tables
SELECT 
	db_name(ius.database_id) AS DatabaseName,
	t.NAME AS TableName,
	SUM(ius.user_seeks + ius.user_scans + ius.user_lookups) AS NbrTimesAccessed
FROM sys.dm_db_index_usage_stats ius
INNER JOIN sys.tables t ON t.OBJECT_ID = ius.object_id
WHERE database_id = DB_ID()
GROUP BY database_id, t.name
ORDER BY SUM(ius.user_seeks + ius.user_scans + ius.user_lookups) DESC


--Get most used indexes
SELECT 
	db_name(ius.database_id) AS DatabaseName,
	t.NAME AS TableName,
	i.NAME AS IndexName,
	i.type_desc AS IndexType,
	ius.user_seeks + ius.user_scans + ius.user_lookups AS NbrTimesAccessed
FROM sys.dm_db_index_usage_stats ius
INNER JOIN sys.indexes i ON i.OBJECT_ID = ius.OBJECT_ID AND i.index_id = ius.index_id
INNER JOIN sys.tables t ON t.OBJECT_ID = i.object_id
WHERE database_id = DB_ID()
ORDER BY ius.user_seeks + ius.user_scans + ius.user_lookups DESC

-- If SQL 2016 or above you can get info on current SQL's being executed by each SPID - this is similar to sp_who2 output but useful if you 
-- want to play with the where clause etc.

SELECT *
FROM sys.dm_exec_requests AS Req
JOIN sys.dm_exec_sessions AS Ses 
   ON Ses.session_id = Req.session_id
CROSS APPLY sys.dm_exec_input_buffer(Req.session_id, Req.request_id) AS InBuf
WHERE
    Ses.session_id>50 and Ses.is_user_process = 1
GO

--Top 10 queries by CPU
SELECT TOP 10 
 [Average CPU used] = total_worker_time / qs.execution_count
,[Total CPU used] = total_worker_time
,[Execution count] = qs.execution_count
,[Individual Query] = SUBSTRING (qt.text,qs.statement_start_offset/2, 
         (CASE WHEN qs.statement_end_offset = -1 
            THEN LEN(CONVERT(NVARCHAR(MAX), qt.text)) * 2 
          ELSE qs.statement_end_offset END - 
qs.statement_start_offset)/2)
,[Parent Query] = qt.text
,DatabaseName = DB_NAME(qt.dbid)
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) as qt
ORDER BY [Average CPU used] DESC;

--OR 

SELECT TOP 5 query_stats.query_hash AS "Query Hash",   
    SUM(query_stats.total_worker_time) / SUM(query_stats.execution_count) AS "Avg CPU Time",  
    MIN(query_stats.statement_text) AS "Statement Text"  
FROM   
    (SELECT QS.*,   
    SUBSTRING(ST.text, (QS.statement_start_offset/2) + 1,  
    ((CASE statement_end_offset   
        WHEN -1 THEN DATALENGTH(ST.text)  
        ELSE QS.statement_end_offset END   
            - QS.statement_start_offset)/2) + 1) AS statement_text  
     FROM sys.dm_exec_query_stats AS QS  
     CROSS APPLY sys.dm_exec_sql_text(QS.sql_handle) as ST) as query_stats  
GROUP BY query_stats.query_hash  
ORDER BY 2 DESC;  

-- Find all current plans that have missing indexes

;WITH XMLNAMESPACES(DEFAULT N'http://schemas.microsoft.com/sqlserver/2004/07/showplan')       
SELECT dec.usecounts, dec.refcounts, dec.objtype
      ,dec.cacheobjtype, des.dbid, des.text      
      ,deq.query_plan 
FROM sys.dm_exec_cached_plans AS dec 
     CROSS APPLY sys.dm_exec_sql_text(dec.plan_handle) AS des 
     CROSS APPLY sys.dm_exec_query_plan(dec.plan_handle) AS deq 
WHERE deq.query_plan.exist(N'/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/QueryPlan/MissingIndexes/MissingIndexGroup') <> 0 
ORDER BY dec.usecounts DESC 


-- Find and fix plans with clustered index seeks and key lookups.  
-- Focus just on key lookups, because they can be easily fixed by adjusting indexes.
--  NOTE: It might take a few minutes to run this on a large plan cache.


;WITH XMLNAMESPACES(DEFAULT N'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
SELECT 
cp.query_hash, cp.query_plan_hash,
PhysicalOperator = operators.value('@PhysicalOp','nvarchar(50)'), 
LogicalOp = operators.value('@LogicalOp','nvarchar(50)'),
AvgRowSize = operators.value('@AvgRowSize','nvarchar(50)'),
EstimateCPU = operators.value('@EstimateCPU','nvarchar(50)'),
EstimateIO = operators.value('@EstimateIO','nvarchar(50)'),
EstimateRebinds = operators.value('@EstimateRebinds','nvarchar(50)'),
EstimateRewinds = operators.value('@EstimateRewinds','nvarchar(50)'),
EstimateRows = operators.value('@EstimateRows','nvarchar(50)'),
Parallel = operators.value('@Parallel','nvarchar(50)'),
NodeId = operators.value('@NodeId','nvarchar(50)'),
EstimatedTotalSubtreeCost = operators.value('@EstimatedTotalSubtreeCost','nvarchar(50)')
FROM sys.dm_exec_query_stats cp
CROSS APPLY sys.dm_exec_query_plan(cp.plan_handle) qp
CROSS APPLY query_plan.nodes('//RelOp') rel(operators)

--Most executed queries
SELECT TOP 10 
 [Execution count] = execution_count
,[Individual Query] = SUBSTRING (qt.text,qs.statement_start_offset/2, 
         (CASE WHEN qs.statement_end_offset = -1 
            THEN LEN(CONVERT(NVARCHAR(MAX), qt.text)) * 2 
          ELSE qs.statement_end_offset END - qs.statement_start_offset)/2)
,[Parent Query] = qt.text
,DatabaseName = DB_NAME(qt.dbid)
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) as qt
WHERE DB_NAME(qt.dbid) NOT IN ( 'BizTalkMsgBoxDb','BizTalkMgmtDb', 'SSODB', 'msdb', 'BAMPrimaryImport','BizTalkRuleEngineDb')
ORDER BY [Execution count] DESC;


-- Queries often blocked
SELECT TOP 10 
 [Average Time Blocked] = (total_elapsed_time - total_worker_time) / qs.execution_count
,[Total Time Blocked] = total_elapsed_time - total_worker_time 
,[Execution count] = qs.execution_count
,[Individual Query] = SUBSTRING (qt.text,qs.statement_start_offset/2, 
         (CASE WHEN qs.statement_end_offset = -1 
            THEN LEN(CONVERT(NVARCHAR(MAX), qt.text)) * 2 
          ELSE qs.statement_end_offset END - qs.statement_start_offset)/2) 
,[Parent Query] = qt.text
,DatabaseName = DB_NAME(qt.dbid)
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) as qt
ORDER BY [Average Time Blocked] DESC;



-- Queries that were parrelised

SELECT
p.dbid,
p.objectid,
p.query_plan,
q.encrypted,
q.TEXT,
cp.usecounts,
cp.size_in_bytes,
cp.plan_handle
FROM sys.dm_exec_cached_plans cp
CROSS APPLY sys.dm_exec_query_plan(cp.plan_handle) AS p
CROSS APPLY sys.dm_exec_sql_text(cp.plan_handle) AS q
WHERE cp.cacheobjtype = 'Compiled Plan' AND p.query_plan.value('declare namespace
p="http://schemas.microsoft.com/sqlserver/2004/07/showplan"; max(//p:RelOp/@Parallel)', 'float') > 0


-- Queries where the amount of time spent by the workers are more than the execution time

SELECT
qs.sql_handle,
qs.statement_start_offset,
qs.statement_end_offset,
q.dbid,
q.objectid,
q.number,
q.encrypted,
q.TEXT
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.plan_handle) AS q
WHERE qs.total_worker_time > qs.total_elapsed_time

-- Returning row count aggregates for a query

SELECT qs.execution_count,  
    SUBSTRING(qt.text,qs.statement_start_offset/2 +1,   
                 (CASE WHEN qs.statement_end_offset = -1   
                       THEN LEN(CONVERT(nvarchar(max), qt.text)) * 2   
                       ELSE qs.statement_end_offset end -  
                            qs.statement_start_offset  
                 )/2  
             ) AS query_text,   
     qt.dbid, dbname= DB_NAME (qt.dbid), qt.objectid,   
     qs.total_rows, qs.last_rows, qs.min_rows, qs.max_rows  
FROM sys.dm_exec_query_stats AS qs   
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt   
WHERE qt.text like '%SELECT%'   
ORDER BY qs.execution_count DESC;  

-- Large Tables

select top 100
             s.name as schema_name
            ,o.name as object_name
            ,sum(reserved_page_count) * 8.0/1024/1024 as reserved_page_GB
            ,sum(used_page_count) * 8.0/1024/1024 as used_page_GB
            ,sum(reserved_page_count - used_page_count) * 8.0/1024/1024 as unused_page_GB
from
                        sys.indexes       i
join       sys.dm_db_partition_stats           ps                     on                     i.object_id=ps.object_id and i.index_id=ps.index_id
join       sys.data_spaces             ds                                                         on                     i.data_space_id=ds.data_space_id
join       sys.objects        o                                                                                  on                     i.object_id=o.object_id
join       sys.schemas      s                                                                                   on                     o.schema_id=s.schema_id
where
            s.name <> 'sys'
group by
             s.name
            ,o.name
order by
            reserved_page_GB desc


-- SQL Server Job Status


SELECT TOP 20

SJ.name 'JOB Name'

,'Run date : ' +

REPLACE(CONVERT(varchar,convert(datetime,convert(varchar,run_date)),102),'.','-')+' '+

SUBSTRING(RIGHT('000000'+CONVERT(varchar,run_time),6),1,2)+':'+SUBSTRING(RIGHT('000000'+CONVERT(varchar,run_time),6),3,2)+':'+SUBSTRING(RIGHT('000000'+CONVERT(varchar,run_time),6),5,2) 'Start Date Time'

,SUBSTRING(RIGHT('000000'+CONVERT(varchar,run_duration),6),1,2)+':'+SUBSTRING(RIGHT('000000'+CONVERT(varchar,run_duration),6),3,2)+':'+SUBSTRING(RIGHT('000000'+CONVERT(varchar,run_duration),6),5,2) 'Duration'

,CASE run_status WHEN 1 THEN '1-SUCCESS' WHEN 0 THEN '0-FAILED' ELSE CONVERT(varchar,run_status) END AS 'Status'

,Step_id

,[Message]

,[Server]

FROM MSDB..SysJobHistory SJH

RIGHT JOIN MSDB..SysJobs SJ

ON SJ.Job_Id = SJH.job_id

WHERE SJ.name LIKE '%3E%' 

AND Step_ID = 0 --Comment this line if you want to see the status of each step of the job

ORDER BY run_date DESC, run_time DESC, step_ID DESC


