/*
    Spotify Account Data - Table Creation
    Normalized tables for Spotify Account Data export.

    Run AFTER 01_create_database.sql
*/

SET QUOTED_IDENTIFIER ON;
GO

USE [Spotify];
GO

-- ============================================================
-- User Profile (single-row reference table)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.UserProfile') AND type = 'U')
CREATE TABLE dbo.UserProfile (
    UserProfileId   INT IDENTITY(1,1) NOT NULL,
    Username        NVARCHAR(100)     NOT NULL,
    Email           NVARCHAR(200)     NULL,
    Country         CHAR(2)           NULL,
    Birthdate       DATE              NULL,
    Gender          VARCHAR(20)       NULL,
    CreationTime    DATE              NULL,
    DisplayName     NVARCHAR(200)     NULL,
    ImageUrl        NVARCHAR(500)     NULL,
    TasteMaker      BIT               NULL,
    Verified        BIT               NULL,
    CONSTRAINT PK_UserProfile PRIMARY KEY CLUSTERED (UserProfileId)
);
GO

-- ============================================================
-- Followers / Following
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Follow') AND type = 'U')
CREATE TABLE dbo.Follow (
    FollowId        INT IDENTITY(1,1) NOT NULL,
    Relationship    VARCHAR(20)       NOT NULL,  -- 'following', 'follower', 'blocking'
    Username        NVARCHAR(200)     NOT NULL,
    CONSTRAINT PK_Follow PRIMARY KEY CLUSTERED (FollowId)
);
GO

-- ============================================================
-- Inferences (Spotify ad targeting segments)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Inference') AND type = 'U')
CREATE TABLE dbo.Inference (
    InferenceId     INT IDENTITY(1,1) NOT NULL,
    InferenceValue  NVARCHAR(500)     NOT NULL,
    CONSTRAINT PK_Inference PRIMARY KEY CLUSTERED (InferenceId)
);
GO

-- ============================================================
-- Marquee (Artist engagement segments)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Marquee') AND type = 'U')
CREATE TABLE dbo.Marquee (
    MarqueeId       INT IDENTITY(1,1) NOT NULL,
    ArtistName      NVARCHAR(200)     NOT NULL,
    Segment         NVARCHAR(100)     NOT NULL,
    CONSTRAINT PK_Marquee PRIMARY KEY CLUSTERED (MarqueeId)
);
GO

-- ============================================================
-- Search Queries
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.SearchQuery') AND type = 'U')
CREATE TABLE dbo.SearchQuery (
    SearchQueryId       INT IDENTITY(1,1) NOT NULL,
    Platform            NVARCHAR(50)      NULL,
    SearchTime          DATETIMEOFFSET(3) NOT NULL,
    SearchQueryText     NVARCHAR(500)     NOT NULL,
    CONSTRAINT PK_SearchQuery PRIMARY KEY CLUSTERED (SearchQueryId)
);
GO

-- Search interaction URIs (what user clicked from search results)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.SearchInteraction') AND type = 'U')
CREATE TABLE dbo.SearchInteraction (
    SearchInteractionId INT IDENTITY(1,1) NOT NULL,
    SearchQueryId       INT               NOT NULL,
    InteractionUri      VARCHAR(200)      NOT NULL,
    CONSTRAINT PK_SearchInteraction PRIMARY KEY CLUSTERED (SearchInteractionId),
    CONSTRAINT FK_SearchInteraction_Query FOREIGN KEY (SearchQueryId) REFERENCES dbo.SearchQuery(SearchQueryId)
);
GO

-- ============================================================
-- Playlists and Playlist Tracks
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Playlist') AND type = 'U')
CREATE TABLE dbo.Playlist (
    PlaylistId          INT IDENTITY(1,1) NOT NULL,
    PlaylistName        NVARCHAR(300)     NOT NULL,
    LastModifiedDate    DATE              NULL,
    NumberOfFollowers   INT               NULL,
    SourceFile          NVARCHAR(100)     NULL,
    CONSTRAINT PK_Playlist PRIMARY KEY CLUSTERED (PlaylistId)
);
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.PlaylistCollaborator') AND type = 'U')
CREATE TABLE dbo.PlaylistCollaborator (
    PlaylistCollaboratorId INT IDENTITY(1,1) NOT NULL,
    PlaylistId             INT               NOT NULL,
    Username               NVARCHAR(200)     NOT NULL,
    CONSTRAINT PK_PlaylistCollaborator PRIMARY KEY CLUSTERED (PlaylistCollaboratorId),
    CONSTRAINT FK_PlaylistCollab_Playlist FOREIGN KEY (PlaylistId) REFERENCES dbo.Playlist(PlaylistId)
);
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.PlaylistTrack') AND type = 'U')
CREATE TABLE dbo.PlaylistTrack (
    PlaylistTrackId INT IDENTITY(1,1) NOT NULL,
    PlaylistId      INT               NOT NULL,
    TrackUri        VARCHAR(50)       NULL,
    TrackName       NVARCHAR(300)     NULL,
    ArtistName      NVARCHAR(200)     NULL,
    AlbumName       NVARCHAR(250)     NULL,
    AddedDate       DATE              NULL,
    TrackId         INT               NULL,       -- FK to Track if URI matches
    CONSTRAINT PK_PlaylistTrack PRIMARY KEY CLUSTERED (PlaylistTrackId),
    CONSTRAINT FK_PlaylistTrack_Playlist FOREIGN KEY (PlaylistId) REFERENCES dbo.Playlist(PlaylistId),
    CONSTRAINT FK_PlaylistTrack_Track FOREIGN KEY (TrackId) REFERENCES dbo.Track(TrackId)
);
GO

-- ============================================================
-- Library (saved/liked items)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.LibraryTrack') AND type = 'U')
CREATE TABLE dbo.LibraryTrack (
    LibraryTrackId  INT IDENTITY(1,1) NOT NULL,
    TrackUri        VARCHAR(50)       NOT NULL,
    TrackName       NVARCHAR(300)     NOT NULL,
    ArtistName      NVARCHAR(200)     NOT NULL,
    AlbumName       NVARCHAR(250)     NULL,
    TrackId         INT               NULL,       -- FK to Track if URI matches
    CONSTRAINT PK_LibraryTrack PRIMARY KEY CLUSTERED (LibraryTrackId),
    CONSTRAINT FK_LibraryTrack_Track FOREIGN KEY (TrackId) REFERENCES dbo.Track(TrackId)
);
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.LibraryAlbum') AND type = 'U')
CREATE TABLE dbo.LibraryAlbum (
    LibraryAlbumId  INT IDENTITY(1,1) NOT NULL,
    AlbumUri        VARCHAR(50)       NOT NULL,
    AlbumName       NVARCHAR(250)     NOT NULL,
    ArtistName      NVARCHAR(200)     NOT NULL,
    CONSTRAINT PK_LibraryAlbum PRIMARY KEY CLUSTERED (LibraryAlbumId)
);
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.LibraryArtist') AND type = 'U')
CREATE TABLE dbo.LibraryArtist (
    LibraryArtistId INT IDENTITY(1,1) NOT NULL,
    ArtistUri       VARCHAR(50)       NOT NULL,
    ArtistName      NVARCHAR(200)     NOT NULL,
    CONSTRAINT PK_LibraryArtist PRIMARY KEY CLUSTERED (LibraryArtistId)
);
GO

-- ============================================================
-- Streaming History (Account Data version - simplified)
-- This overlaps with the Extended History but covers different date ranges
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.StreamingHistoryMusic') AND type = 'U')
CREATE TABLE dbo.StreamingHistoryMusic (
    StreamingHistoryMusicId INT IDENTITY(1,1) NOT NULL,
    EndTime                 DATETIME2(0)      NOT NULL,
    ArtistName              NVARCHAR(200)     NOT NULL,
    TrackName               NVARCHAR(300)     NOT NULL,
    MsPlayed                INT               NOT NULL,
    SourceFile              NVARCHAR(100)     NULL,
    CONSTRAINT PK_StreamingHistoryMusic PRIMARY KEY CLUSTERED (StreamingHistoryMusicId)
);
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.StreamingHistoryPodcast') AND type = 'U')
CREATE TABLE dbo.StreamingHistoryPodcast (
    StreamingHistoryPodcastId INT IDENTITY(1,1) NOT NULL,
    EndTime                   DATETIME2(0)      NOT NULL,
    PodcastName               NVARCHAR(200)     NOT NULL,
    EpisodeName               NVARCHAR(300)     NOT NULL,
    MsPlayed                  INT               NOT NULL,
    SourceFile                NVARCHAR(100)     NULL,
    CONSTRAINT PK_StreamingHistoryPodcast PRIMARY KEY CLUSTERED (StreamingHistoryPodcastId)
);
GO

-- ============================================================
-- Indexes
-- ============================================================
CREATE NONCLUSTERED INDEX IX_SearchQuery_Time ON dbo.SearchQuery (SearchTime);
GO
CREATE NONCLUSTERED INDEX IX_PlaylistTrack_TrackUri ON dbo.PlaylistTrack (TrackUri);
GO
CREATE NONCLUSTERED INDEX IX_PlaylistTrack_PlaylistId ON dbo.PlaylistTrack (PlaylistId);
GO
CREATE NONCLUSTERED INDEX IX_LibraryTrack_TrackUri ON dbo.LibraryTrack (TrackUri);
GO
CREATE NONCLUSTERED INDEX IX_StreamingHistoryMusic_EndTime ON dbo.StreamingHistoryMusic (EndTime);
GO
CREATE NONCLUSTERED INDEX IX_Marquee_ArtistName ON dbo.Marquee (ArtistName);
GO

PRINT 'Account Data tables created successfully.';
GO
