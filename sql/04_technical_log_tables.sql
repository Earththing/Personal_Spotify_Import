/*
    Spotify Technical Log Information - Table Creation
    Imports the most analytically valuable technical log files.

    Run AFTER 01_create_database.sql
*/

SET QUOTED_IDENTIFIER ON;
GO

USE [Spotify];
GO

-- ============================================================
-- Collection Changes (tracks/albums added/removed from library)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.CollectionChange') AND type = 'U')
CREATE TABLE dbo.CollectionChange (
    CollectionChangeId  BIGINT IDENTITY(1,1) NOT NULL,
    ChangeTime          DATETIMEOFFSET(3)    NOT NULL,
    ChangeType          VARCHAR(10)          NOT NULL,  -- 'added' or 'removed'
    CollectionSet       VARCHAR(20)          NULL,      -- 'collection' etc.
    ItemUri             VARCHAR(100)         NULL,
    ContextUri          VARCHAR(100)         NULL,
    CONSTRAINT PK_CollectionChange PRIMARY KEY CLUSTERED (CollectionChangeId)
);
GO

-- ============================================================
-- Playlist Changes (tracks added/removed from playlists)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.PlaylistChange') AND type = 'U')
CREATE TABLE dbo.PlaylistChange (
    PlaylistChangeId    BIGINT IDENTITY(1,1) NOT NULL,
    ChangeTime          DATETIMEOFFSET(3)    NOT NULL,
    ChangeType          VARCHAR(10)          NOT NULL,  -- 'added' or 'removed'
    PlaylistUri         VARCHAR(100)         NULL,
    ItemUri             VARCHAR(100)         NULL,
    ItemUriKind         VARCHAR(20)          NULL,
    ClientPlatform      NVARCHAR(50)         NULL,
    CONSTRAINT PK_PlaylistChange PRIMARY KEY CLUSTERED (PlaylistChangeId)
);
GO

-- ============================================================
-- Rootlist Changes (playlists/folders added/removed from library)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.RootlistChange') AND type = 'U')
CREATE TABLE dbo.RootlistChange (
    RootlistChangeId    BIGINT IDENTITY(1,1) NOT NULL,
    ChangeTime          DATETIMEOFFSET(3)    NOT NULL,
    ChangeType          VARCHAR(10)          NOT NULL,  -- 'added' or 'removed'
    ItemUri             VARCHAR(100)         NULL,
    ItemUriKind         VARCHAR(30)          NULL,
    ClientPlatform      NVARCHAR(50)         NULL,
    CONSTRAINT PK_RootlistChange PRIMARY KEY CLUSTERED (RootlistChangeId)
);
GO

-- ============================================================
-- Share Events
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.ShareEvent') AND type = 'U')
CREATE TABLE dbo.ShareEvent (
    ShareEventId        BIGINT IDENTITY(1,1) NOT NULL,
    ShareTime           DATETIMEOFFSET(3)    NOT NULL,
    EntityUri           VARCHAR(100)         NULL,
    DestinationId       NVARCHAR(100)        NULL,
    ShareId             VARCHAR(100)         NULL,
    SourcePage          NVARCHAR(100)        NULL,
    SourcePageUri       VARCHAR(200)         NULL,
    DeviceType          NVARCHAR(50)         NULL,
    OsName              NVARCHAR(50)         NULL,
    OsVersion           NVARCHAR(50)         NULL,
    Country             CHAR(2)              NULL,
    CONSTRAINT PK_ShareEvent PRIMARY KEY CLUSTERED (ShareEventId)
);
GO

-- ============================================================
-- Playback Errors
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.PlaybackError') AND type = 'U')
CREATE TABLE dbo.PlaybackError (
    PlaybackErrorId     BIGINT IDENTITY(1,1) NOT NULL,
    ErrorTime           DATETIMEOFFSET(3)    NOT NULL,
    FileId              VARCHAR(100)         NULL,
    TrackId             VARCHAR(100)         NULL,
    ErrorCode           VARCHAR(50)          NULL,
    IsFatal             BIT                  NULL,
    Bitrate             INT                  NULL,
    DeviceType          NVARCHAR(50)         NULL,
    OsName              NVARCHAR(50)         NULL,
    CONSTRAINT PK_PlaybackError PRIMARY KEY CLUSTERED (PlaybackErrorId)
);
GO

-- ============================================================
-- Session Creation
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Session') AND type = 'U')
CREATE TABLE dbo.Session (
    SessionId           BIGINT IDENTITY(1,1) NOT NULL,
    SessionTime         DATETIMEOFFSET(3)    NOT NULL,
    SpotifySessionId    VARCHAR(100)         NULL,
    CreatedAt           NVARCHAR(50)         NULL,
    CONSTRAINT PK_Session PRIMARY KEY CLUSTERED (SessionId)
);
GO

-- ============================================================
-- Account Pages Activity
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.AccountActivity') AND type = 'U')
CREATE TABLE dbo.AccountActivity (
    AccountActivityId   BIGINT IDENTITY(1,1) NOT NULL,
    ActivityTime        DATETIMEOFFSET(3)    NOT NULL,
    ActivityName        NVARCHAR(200)        NULL,
    Market              NVARCHAR(10)         NULL,
    Success             BIT                  NULL,
    Reason              NVARCHAR(200)        NULL,
    DeviceType          NVARCHAR(50)         NULL,
    OsName              NVARCHAR(50)         NULL,
    Country             CHAR(2)              NULL,
    CONSTRAINT PK_AccountActivity PRIMARY KEY CLUSTERED (AccountActivityId)
);
GO

-- ============================================================
-- Indexes
-- ============================================================
CREATE NONCLUSTERED INDEX IX_CollectionChange_Time ON dbo.CollectionChange (ChangeTime);
GO
CREATE NONCLUSTERED INDEX IX_PlaylistChange_Time ON dbo.PlaylistChange (ChangeTime);
GO
CREATE NONCLUSTERED INDEX IX_ShareEvent_Time ON dbo.ShareEvent (ShareTime);
GO
CREATE NONCLUSTERED INDEX IX_Session_Time ON dbo.Session (SessionTime);
GO

PRINT 'Technical Log tables created successfully.';
GO
