/*
    Spotify Extended Streaming History - Database Creation
    Creates the 'Spotify' database with normalized tables for streaming history data.

    Run this script against your SQL Server instance with Windows Authentication.
    Requires sysadmin or dbcreator role.
*/

SET QUOTED_IDENTIFIER ON;
GO

USE [master];
GO

-- Create database if it doesn't exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'Spotify')
BEGIN
    CREATE DATABASE [Spotify];
END;
GO

USE [Spotify];
GO

-- ============================================================
-- Dimension Tables
-- ============================================================

-- Artists
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Artist') AND type = 'U')
CREATE TABLE dbo.Artist (
    ArtistId        INT IDENTITY(1,1) NOT NULL,
    ArtistName      NVARCHAR(200)     NOT NULL,
    CONSTRAINT PK_Artist PRIMARY KEY CLUSTERED (ArtistId),
    CONSTRAINT UQ_Artist_Name UNIQUE (ArtistName)
);
GO

-- Albums (an album is unique per artist+album name combo)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Album') AND type = 'U')
CREATE TABLE dbo.Album (
    AlbumId         INT IDENTITY(1,1) NOT NULL,
    AlbumName       NVARCHAR(250)     NOT NULL,
    ArtistId        INT               NOT NULL,
    CONSTRAINT PK_Album PRIMARY KEY CLUSTERED (AlbumId),
    CONSTRAINT FK_Album_Artist FOREIGN KEY (ArtistId) REFERENCES dbo.Artist(ArtistId),
    CONSTRAINT UQ_Album_Artist UNIQUE (AlbumName, ArtistId)
);
GO

-- Tracks
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Track') AND type = 'U')
CREATE TABLE dbo.Track (
    TrackId         INT IDENTITY(1,1) NOT NULL,
    SpotifyUri      VARCHAR(50)       NOT NULL,
    TrackName       NVARCHAR(200)     NOT NULL,
    AlbumId         INT               NULL,
    ArtistId        INT               NULL,
    CONSTRAINT PK_Track PRIMARY KEY CLUSTERED (TrackId),
    CONSTRAINT FK_Track_Album FOREIGN KEY (AlbumId) REFERENCES dbo.Album(AlbumId),
    CONSTRAINT FK_Track_Artist FOREIGN KEY (ArtistId) REFERENCES dbo.Artist(ArtistId),
    CONSTRAINT UQ_Track_Uri UNIQUE (SpotifyUri)
);
GO

-- Podcast Shows
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.PodcastShow') AND type = 'U')
CREATE TABLE dbo.PodcastShow (
    ShowId          INT IDENTITY(1,1) NOT NULL,
    ShowName        NVARCHAR(100)     NOT NULL,
    CONSTRAINT PK_PodcastShow PRIMARY KEY CLUSTERED (ShowId),
    CONSTRAINT UQ_PodcastShow_Name UNIQUE (ShowName)
);
GO

-- Podcast Episodes
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.PodcastEpisode') AND type = 'U')
CREATE TABLE dbo.PodcastEpisode (
    EpisodeId       INT IDENTITY(1,1) NOT NULL,
    SpotifyUri      VARCHAR(50)       NOT NULL,
    EpisodeName     NVARCHAR(200)     NOT NULL,
    ShowId          INT               NULL,
    CONSTRAINT PK_PodcastEpisode PRIMARY KEY CLUSTERED (EpisodeId),
    CONSTRAINT FK_PodcastEpisode_Show FOREIGN KEY (ShowId) REFERENCES dbo.PodcastShow(ShowId),
    CONSTRAINT UQ_PodcastEpisode_Uri UNIQUE (SpotifyUri)
);
GO

-- Audiobooks
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Audiobook') AND type = 'U')
CREATE TABLE dbo.Audiobook (
    AudiobookId     INT IDENTITY(1,1) NOT NULL,
    SpotifyUri      VARCHAR(50)       NOT NULL,
    Title           NVARCHAR(100)     NOT NULL,
    CONSTRAINT PK_Audiobook PRIMARY KEY CLUSTERED (AudiobookId),
    CONSTRAINT UQ_Audiobook_Uri UNIQUE (SpotifyUri)
);
GO

-- Audiobook Chapters
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.AudiobookChapter') AND type = 'U')
CREATE TABLE dbo.AudiobookChapter (
    ChapterId       INT IDENTITY(1,1) NOT NULL,
    SpotifyUri      VARCHAR(50)       NOT NULL,
    ChapterTitle    NVARCHAR(300)     NOT NULL,
    AudiobookId     INT               NULL,
    CONSTRAINT PK_AudiobookChapter PRIMARY KEY CLUSTERED (ChapterId),
    CONSTRAINT FK_AudiobookChapter_Audiobook FOREIGN KEY (AudiobookId) REFERENCES dbo.Audiobook(AudiobookId),
    CONSTRAINT UQ_AudiobookChapter_Uri UNIQUE (SpotifyUri)
);
GO

-- ============================================================
-- Fact Table
-- ============================================================

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Play') AND type = 'U')
CREATE TABLE dbo.Play (
    PlayId              BIGINT IDENTITY(1,1) NOT NULL,
    Timestamp           DATETIMEOFFSET(0)    NOT NULL,
    Platform            NVARCHAR(100)        NULL,
    MsPlayed            INT                  NOT NULL,
    ConnCountry         CHAR(2)              NULL,
    IpAddr              VARCHAR(45)          NULL,
    TrackId             INT                  NULL,
    EpisodeId           INT                  NULL,
    AudiobookChapterId  INT                  NULL,
    ReasonStart         VARCHAR(30)          NULL,
    ReasonEnd           VARCHAR(30)          NULL,
    Shuffle             BIT                  NULL,
    Skipped             BIT                  NULL,
    Offline             BIT                  NULL,
    OfflineTimestamp     BIGINT               NULL,
    IncognitoMode       BIT                  NULL,
    SourceFile          NVARCHAR(200)        NULL,
    CONSTRAINT PK_Play PRIMARY KEY CLUSTERED (PlayId),
    CONSTRAINT FK_Play_Track FOREIGN KEY (TrackId) REFERENCES dbo.Track(TrackId),
    CONSTRAINT FK_Play_Episode FOREIGN KEY (EpisodeId) REFERENCES dbo.PodcastEpisode(EpisodeId),
    CONSTRAINT FK_Play_AudiobookChapter FOREIGN KEY (AudiobookChapterId) REFERENCES dbo.AudiobookChapter(ChapterId)
);
GO

-- ============================================================
-- Indexes for common query patterns
-- ============================================================

CREATE NONCLUSTERED INDEX IX_Play_Timestamp ON dbo.Play (Timestamp);
GO
CREATE NONCLUSTERED INDEX IX_Play_TrackId ON dbo.Play (TrackId) WHERE TrackId IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_Play_EpisodeId ON dbo.Play (EpisodeId) WHERE EpisodeId IS NOT NULL;
GO
CREATE NONCLUSTERED INDEX IX_Track_ArtistId ON dbo.Track (ArtistId);
GO
CREATE NONCLUSTERED INDEX IX_Track_AlbumId ON dbo.Track (AlbumId);
GO
CREATE NONCLUSTERED INDEX IX_Album_ArtistId ON dbo.Album (ArtistId);
GO

PRINT 'Spotify database and all tables created successfully.';
GO
