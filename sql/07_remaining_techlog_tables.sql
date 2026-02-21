/*
    Spotify Technical Log Information - Remaining Tables

    Uses a single generic table for ALL remaining technical log files.
    Common context columns are extracted into typed columns.
    Message-specific fields are stored as JSON in MessageData.

    This approach avoids creating 150+ individual tables while still
    allowing SQL Server JSON functions for querying specific fields:

      SELECT JSON_VALUE(MessageData, '$.content_uri') AS ContentUri
      FROM dbo.TechLogEvent
      WHERE LogType = 'RawCoreStream'

    Run AFTER 04_technical_log_tables.sql
*/

SET QUOTED_IDENTIFIER ON;
GO

USE [Spotify];
GO

-- ============================================================
-- Generic Technical Log Event table
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.TechLogEvent') AND type = 'U')
CREATE TABLE dbo.TechLogEvent (
    TechLogEventId      BIGINT IDENTITY(1,1) NOT NULL,
    LogType             VARCHAR(100)         NOT NULL,   -- Derived from filename (e.g., 'RawCoreStream')
    TimestampUtc        DATETIMEOFFSET(3)    NULL,       -- From timestamp_utc
    ContextTime         BIGINT               NULL,       -- Epoch ms from context_time
    AppVersion          NVARCHAR(50)         NULL,       -- context_application_version
    ConnCountry         CHAR(2)              NULL,       -- context_conn_country
    DeviceManufacturer  NVARCHAR(100)        NULL,       -- context_device_manufacturer
    DeviceModel         NVARCHAR(100)        NULL,       -- context_device_model
    DeviceType          NVARCHAR(50)         NULL,       -- context_device_type
    OsName              NVARCHAR(50)         NULL,       -- context_os_name
    OsVersion           NVARCHAR(50)         NULL,       -- context_os_version
    UserAgent           NVARCHAR(500)        NULL,       -- context_user_agent
    MessageData         NVARCHAR(MAX)        NULL,       -- JSON object of all message_* fields
    SourceFile          NVARCHAR(200)        NULL,       -- Original filename
    CONSTRAINT PK_TechLogEvent PRIMARY KEY CLUSTERED (TechLogEventId)
);
GO

-- ============================================================
-- Indexes for common query patterns
-- ============================================================
CREATE NONCLUSTERED INDEX IX_TechLogEvent_LogType ON dbo.TechLogEvent (LogType);
GO
CREATE NONCLUSTERED INDEX IX_TechLogEvent_Timestamp ON dbo.TechLogEvent (TimestampUtc) WHERE TimestampUtc IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_TechLogEvent_LogType_Timestamp ON dbo.TechLogEvent (LogType, TimestampUtc);
GO

-- ============================================================
-- Recipients (data-sharing disclosure - special structure)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.DataRecipient') AND type = 'U')
CREATE TABLE dbo.DataRecipient (
    DataRecipientId INT IDENTITY(1,1) NOT NULL,
    GroupName       NVARCHAR(200)     NULL,
    MemberName      NVARCHAR(300)     NOT NULL,
    CONSTRAINT PK_DataRecipient PRIMARY KEY CLUSTERED (DataRecipientId)
);
GO

PRINT 'Remaining Technical Log tables created successfully.';
GO
