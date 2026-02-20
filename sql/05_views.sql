/*
    Spotify Database - Views
    Meaningful views across all data sources:
    - Streaming History (Extended)
    - Account Data
    - Technical Logs

    Run AFTER all table creation scripts and imports.
*/

SET QUOTED_IDENTIFIER ON;
GO

USE [Spotify];
GO

-- ============================================================
-- STREAMING HISTORY VIEWS
-- ============================================================

-- Full play detail with all dimension data resolved
IF OBJECT_ID('dbo.vw_PlayDetail', 'V') IS NOT NULL DROP VIEW dbo.vw_PlayDetail;
GO
CREATE VIEW dbo.vw_PlayDetail AS
SELECT
    p.PlayId,
    p.Timestamp,
    CAST(p.Timestamp AS DATE)                       AS PlayDate,
    DATEPART(YEAR, p.Timestamp)                     AS PlayYear,
    DATEPART(MONTH, p.Timestamp)                    AS PlayMonth,
    DATENAME(WEEKDAY, p.Timestamp)                  AS DayOfWeek,
    DATEPART(HOUR, p.Timestamp)                     AS HourOfDay,
    p.MsPlayed,
    CAST(p.MsPlayed / 1000.0 AS DECIMAL(12,1))     AS SecondsPlayed,
    CAST(p.MsPlayed / 60000.0 AS DECIMAL(12,2))    AS MinutesPlayed,
    p.Platform,
    p.ConnCountry,
    p.IpAddr,
    -- Content type
    CASE
        WHEN p.TrackId IS NOT NULL             THEN 'Music'
        WHEN p.EpisodeId IS NOT NULL           THEN 'Podcast'
        WHEN p.AudiobookChapterId IS NOT NULL  THEN 'Audiobook'
        ELSE 'Unknown'
    END                                             AS ContentType,
    -- Track info
    t.TrackName,
    t.SpotifyUri                                    AS TrackUri,
    ar.ArtistName,
    al.AlbumName,
    -- Episode info
    ep.EpisodeName,
    ps.ShowName                                     AS PodcastShowName,
    -- Audiobook info
    ac.ChapterTitle                                 AS AudiobookChapterTitle,
    ab.Title                                        AS AudiobookTitle,
    -- Playback context
    p.ReasonStart,
    p.ReasonEnd,
    p.Shuffle,
    p.Skipped,
    p.Offline,
    p.IncognitoMode,
    p.SourceFile
FROM dbo.Play p
LEFT JOIN dbo.Track t           ON p.TrackId = t.TrackId
LEFT JOIN dbo.Artist ar         ON t.ArtistId = ar.ArtistId
LEFT JOIN dbo.Album al          ON t.AlbumId = al.AlbumId
LEFT JOIN dbo.PodcastEpisode ep ON p.EpisodeId = ep.EpisodeId
LEFT JOIN dbo.PodcastShow ps    ON ep.ShowId = ps.ShowId
LEFT JOIN dbo.AudiobookChapter ac ON p.AudiobookChapterId = ac.ChapterId
LEFT JOIN dbo.Audiobook ab      ON ac.AudiobookId = ab.AudiobookId;
GO

-- Artist listening stats
IF OBJECT_ID('dbo.vw_ArtistStats', 'V') IS NOT NULL DROP VIEW dbo.vw_ArtistStats;
GO
CREATE VIEW dbo.vw_ArtistStats AS
SELECT
    ar.ArtistId,
    ar.ArtistName,
    COUNT(*)                                                    AS TotalPlays,
    COUNT(DISTINCT t.TrackId)                                   AS UniqueTracksPlayed,
    COUNT(DISTINCT al.AlbumId)                                  AS UniqueAlbumsPlayed,
    SUM(p.MsPlayed)                                             AS TotalMsPlayed,
    CAST(SUM(p.MsPlayed) / 3600000.0 AS DECIMAL(10,1))         AS TotalHours,
    CAST(AVG(p.MsPlayed) / 1000.0 AS DECIMAL(8,1))             AS AvgSecondsPerPlay,
    MIN(p.Timestamp)                                            AS FirstPlayed,
    MAX(p.Timestamp)                                            AS LastPlayed,
    DATEDIFF(DAY, MIN(p.Timestamp), MAX(p.Timestamp))           AS DaysSpan,
    COUNT(DISTINCT CAST(p.Timestamp AS DATE))                   AS DaysListened,
    SUM(CASE WHEN p.Skipped = 1 THEN 1 ELSE 0 END)             AS SkipCount,
    CAST(SUM(CASE WHEN p.Skipped = 1 THEN 1.0 ELSE 0 END)
         / NULLIF(COUNT(*), 0) * 100 AS DECIMAL(5,1))          AS SkipPct
FROM dbo.Play p
JOIN dbo.Track t  ON p.TrackId = t.TrackId
JOIN dbo.Artist ar ON t.ArtistId = ar.ArtistId
LEFT JOIN dbo.Album al ON t.AlbumId = al.AlbumId
GROUP BY ar.ArtistId, ar.ArtistName;
GO

-- Track listening stats
IF OBJECT_ID('dbo.vw_TrackStats', 'V') IS NOT NULL DROP VIEW dbo.vw_TrackStats;
GO
CREATE VIEW dbo.vw_TrackStats AS
SELECT
    t.TrackId,
    t.TrackName,
    t.SpotifyUri                                                AS TrackUri,
    ar.ArtistName,
    al.AlbumName,
    COUNT(*)                                                    AS TotalPlays,
    SUM(p.MsPlayed)                                             AS TotalMsPlayed,
    CAST(SUM(p.MsPlayed) / 60000.0 AS DECIMAL(10,1))           AS TotalMinutes,
    CAST(AVG(p.MsPlayed) / 1000.0 AS DECIMAL(8,1))             AS AvgSecondsPerPlay,
    MIN(p.Timestamp)                                            AS FirstPlayed,
    MAX(p.Timestamp)                                            AS LastPlayed,
    SUM(CASE WHEN p.Skipped = 1 THEN 1 ELSE 0 END)             AS SkipCount,
    SUM(CASE WHEN p.Shuffle = 1 THEN 1 ELSE 0 END)             AS ShuffleCount,
    CAST(SUM(CASE WHEN p.Skipped = 1 THEN 1.0 ELSE 0 END)
         / NULLIF(COUNT(*), 0) * 100 AS DECIMAL(5,1))          AS SkipPct
FROM dbo.Play p
JOIN dbo.Track t   ON p.TrackId = t.TrackId
LEFT JOIN dbo.Artist ar ON t.ArtistId = ar.ArtistId
LEFT JOIN dbo.Album al  ON t.AlbumId = al.AlbumId
GROUP BY t.TrackId, t.TrackName, t.SpotifyUri, ar.ArtistName, al.AlbumName;
GO

-- Album listening stats
IF OBJECT_ID('dbo.vw_AlbumStats', 'V') IS NOT NULL DROP VIEW dbo.vw_AlbumStats;
GO
CREATE VIEW dbo.vw_AlbumStats AS
SELECT
    al.AlbumId,
    al.AlbumName,
    ar.ArtistName,
    COUNT(*)                                                    AS TotalPlays,
    COUNT(DISTINCT t.TrackId)                                   AS UniqueTracksPlayed,
    SUM(p.MsPlayed)                                             AS TotalMsPlayed,
    CAST(SUM(p.MsPlayed) / 3600000.0 AS DECIMAL(10,1))         AS TotalHours,
    MIN(p.Timestamp)                                            AS FirstPlayed,
    MAX(p.Timestamp)                                            AS LastPlayed
FROM dbo.Play p
JOIN dbo.Track t   ON p.TrackId = t.TrackId
JOIN dbo.Album al  ON t.AlbumId = al.AlbumId
JOIN dbo.Artist ar ON al.ArtistId = ar.ArtistId
GROUP BY al.AlbumId, al.AlbumName, ar.ArtistName;
GO

-- Listening by year/month
IF OBJECT_ID('dbo.vw_MonthlyListening', 'V') IS NOT NULL DROP VIEW dbo.vw_MonthlyListening;
GO
CREATE VIEW dbo.vw_MonthlyListening AS
SELECT
    DATEPART(YEAR, p.Timestamp)                                 AS [Year],
    DATEPART(MONTH, p.Timestamp)                                AS [Month],
    DATEPART(YEAR, p.Timestamp) * 100 + DATEPART(MONTH, p.Timestamp) AS MonthSort,
    FORMAT(p.Timestamp, 'yyyy-MM')                              AS YearMonth,
    COUNT(*)                                                    AS TotalPlays,
    COUNT(DISTINCT CASE WHEN p.TrackId IS NOT NULL THEN p.TrackId END) AS UniqueTracks,
    COUNT(DISTINCT CAST(p.Timestamp AS DATE))                   AS DaysActive,
    SUM(p.MsPlayed)                                             AS TotalMsPlayed,
    CAST(SUM(CAST(p.MsPlayed AS BIGINT)) / 3600000.0 AS DECIMAL(10,1)) AS TotalHours,
    SUM(CASE WHEN p.TrackId IS NOT NULL THEN 1 ELSE 0 END)     AS MusicPlays,
    SUM(CASE WHEN p.EpisodeId IS NOT NULL THEN 1 ELSE 0 END)   AS PodcastPlays,
    SUM(CASE WHEN p.AudiobookChapterId IS NOT NULL THEN 1 ELSE 0 END) AS AudiobookPlays
FROM dbo.Play p
GROUP BY
    DATEPART(YEAR, p.Timestamp),
    DATEPART(MONTH, p.Timestamp),
    DATEPART(YEAR, p.Timestamp) * 100 + DATEPART(MONTH, p.Timestamp),
    FORMAT(p.Timestamp, 'yyyy-MM');
GO

-- Listening by day of week and hour (heatmap data)
IF OBJECT_ID('dbo.vw_ListeningHeatmap', 'V') IS NOT NULL DROP VIEW dbo.vw_ListeningHeatmap;
GO
CREATE VIEW dbo.vw_ListeningHeatmap AS
SELECT
    DATEPART(WEEKDAY, p.Timestamp)                              AS DayOfWeekNum,
    DATENAME(WEEKDAY, p.Timestamp)                              AS DayOfWeek,
    DATEPART(HOUR, p.Timestamp)                                 AS HourOfDay,
    COUNT(*)                                                    AS TotalPlays,
    CAST(SUM(CAST(p.MsPlayed AS BIGINT)) / 60000.0 AS DECIMAL(10,0)) AS TotalMinutes
FROM dbo.Play p
GROUP BY
    DATEPART(WEEKDAY, p.Timestamp),
    DATENAME(WEEKDAY, p.Timestamp),
    DATEPART(HOUR, p.Timestamp);
GO

-- Platform/device usage
IF OBJECT_ID('dbo.vw_PlatformStats', 'V') IS NOT NULL DROP VIEW dbo.vw_PlatformStats;
GO
CREATE VIEW dbo.vw_PlatformStats AS
SELECT
    p.Platform,
    COUNT(*)                                                    AS TotalPlays,
    CAST(SUM(CAST(p.MsPlayed AS BIGINT)) / 3600000.0 AS DECIMAL(10,1)) AS TotalHours,
    MIN(p.Timestamp)                                            AS FirstSeen,
    MAX(p.Timestamp)                                            AS LastSeen
FROM dbo.Play p
GROUP BY p.Platform;
GO

-- Skip analysis by reason
IF OBJECT_ID('dbo.vw_SkipAnalysis', 'V') IS NOT NULL DROP VIEW dbo.vw_SkipAnalysis;
GO
CREATE VIEW dbo.vw_SkipAnalysis AS
SELECT
    p.ReasonEnd,
    COUNT(*)                                                    AS TotalPlays,
    SUM(CASE WHEN p.Skipped = 1 THEN 1 ELSE 0 END)            AS Skipped,
    CAST(AVG(CASE WHEN p.Skipped = 1 THEN p.MsPlayed * 1.0 END) / 1000 AS DECIMAL(8,1)) AS AvgSecondsBeforeSkip,
    CAST(AVG(CASE WHEN p.Skipped = 0 THEN p.MsPlayed * 1.0 END) / 1000 AS DECIMAL(8,1)) AS AvgSecondsFullPlay
FROM dbo.Play p
WHERE p.TrackId IS NOT NULL
GROUP BY p.ReasonEnd;
GO

-- Offline vs online listening
IF OBJECT_ID('dbo.vw_OfflineListening', 'V') IS NOT NULL DROP VIEW dbo.vw_OfflineListening;
GO
CREATE VIEW dbo.vw_OfflineListening AS
SELECT
    DATEPART(YEAR, p.Timestamp)                                 AS [Year],
    p.Offline,
    COUNT(*)                                                    AS TotalPlays,
    CAST(SUM(CAST(p.MsPlayed AS BIGINT)) / 3600000.0 AS DECIMAL(10,1)) AS TotalHours,
    COUNT(DISTINCT CASE WHEN p.TrackId IS NOT NULL THEN p.TrackId END) AS UniqueTracks
FROM dbo.Play p
GROUP BY DATEPART(YEAR, p.Timestamp), p.Offline;
GO

-- ============================================================
-- ACCOUNT DATA VIEWS
-- ============================================================

-- Playlist overview with track counts
IF OBJECT_ID('dbo.vw_PlaylistOverview', 'V') IS NOT NULL DROP VIEW dbo.vw_PlaylistOverview;
GO
CREATE VIEW dbo.vw_PlaylistOverview AS
SELECT
    pl.PlaylistId,
    pl.PlaylistName,
    pl.LastModifiedDate,
    pl.NumberOfFollowers,
    COUNT(pt.PlaylistTrackId)                                   AS TrackCount,
    MIN(pt.AddedDate)                                           AS EarliestTrackAdded,
    MAX(pt.AddedDate)                                           AS LatestTrackAdded,
    COUNT(DISTINCT pt.ArtistName)                               AS UniqueArtists
FROM dbo.Playlist pl
LEFT JOIN dbo.PlaylistTrack pt ON pl.PlaylistId = pt.PlaylistId
GROUP BY pl.PlaylistId, pl.PlaylistName, pl.LastModifiedDate, pl.NumberOfFollowers;
GO

-- Marquee artist engagement with listening data
IF OBJECT_ID('dbo.vw_ArtistEngagement', 'V') IS NOT NULL DROP VIEW dbo.vw_ArtistEngagement;
GO
CREATE VIEW dbo.vw_ArtistEngagement AS
SELECT
    m.ArtistName,
    m.Segment                                                   AS MarqueeSegment,
    a.ArtistId,
    COALESCE(s.TotalPlays, 0)                                   AS TotalPlays,
    COALESCE(s.TotalHours, 0)                                   AS TotalHours,
    COALESCE(s.UniqueTracksPlayed, 0)                           AS UniqueTracksPlayed,
    s.FirstPlayed,
    s.LastPlayed,
    CASE WHEN la.LibraryArtistId IS NOT NULL THEN 1 ELSE 0 END AS InLibrary,
    CASE WHEN f.FollowId IS NOT NULL THEN 1 ELSE 0 END         AS IsFollowed
FROM dbo.Marquee m
LEFT JOIN dbo.Artist a          ON m.ArtistName = a.ArtistName
LEFT JOIN dbo.vw_ArtistStats s  ON a.ArtistId = s.ArtistId
LEFT JOIN dbo.LibraryArtist la  ON m.ArtistName = la.ArtistName
LEFT JOIN dbo.Follow f          ON m.ArtistName = f.Username AND f.Relationship = 'following';
GO

-- Search history with result interaction
IF OBJECT_ID('dbo.vw_SearchHistory', 'V') IS NOT NULL DROP VIEW dbo.vw_SearchHistory;
GO
CREATE VIEW dbo.vw_SearchHistory AS
SELECT
    sq.SearchQueryId,
    sq.SearchTime,
    CAST(sq.SearchTime AS DATE)                                 AS SearchDate,
    sq.Platform,
    sq.SearchQueryText,
    COUNT(si.SearchInteractionId)                               AS ClickCount,
    CASE WHEN COUNT(si.SearchInteractionId) > 0
         THEN 1 ELSE 0 END                                     AS HadInteraction
FROM dbo.SearchQuery sq
LEFT JOIN dbo.SearchInteraction si ON sq.SearchQueryId = si.SearchQueryId
GROUP BY sq.SearchQueryId, sq.SearchTime, sq.Platform, sq.SearchQueryText;
GO

-- ============================================================
-- CROSS-SOURCE VIEWS (spanning multiple data packages)
-- ============================================================

-- Library tracks matched to streaming history
-- Shows which saved tracks you actually listen to (and which you don't)
IF OBJECT_ID('dbo.vw_LibraryListeningStatus', 'V') IS NOT NULL DROP VIEW dbo.vw_LibraryListeningStatus;
GO
CREATE VIEW dbo.vw_LibraryListeningStatus AS
SELECT
    lt.TrackName,
    lt.ArtistName,
    lt.AlbumName,
    lt.TrackUri,
    t.TrackId,
    COALESCE(ts.TotalPlays, 0)                                  AS TotalPlays,
    COALESCE(ts.TotalMinutes, 0)                                AS TotalMinutes,
    ts.FirstPlayed,
    ts.LastPlayed,
    CASE
        WHEN ts.TotalPlays IS NULL               THEN 'Never Played'
        WHEN ts.LastPlayed < DATEADD(MONTH, -6, GETDATE()) THEN 'Dormant (6+ months)'
        WHEN ts.LastPlayed < DATEADD(MONTH, -3, GETDATE()) THEN 'Inactive (3-6 months)'
        ELSE 'Active'
    END                                                         AS ListeningStatus
FROM dbo.LibraryTrack lt
LEFT JOIN dbo.Track t ON lt.TrackUri = t.SpotifyUri
LEFT JOIN dbo.vw_TrackStats ts ON t.TrackId = ts.TrackId;
GO

-- Playlist tracks matched to streaming history
-- Shows which playlist tracks you actually play vs just collected
IF OBJECT_ID('dbo.vw_PlaylistTrackActivity', 'V') IS NOT NULL DROP VIEW dbo.vw_PlaylistTrackActivity;
GO
CREATE VIEW dbo.vw_PlaylistTrackActivity AS
SELECT
    pl.PlaylistName,
    pt.TrackName,
    pt.ArtistName,
    pt.AlbumName,
    pt.AddedDate,
    COALESCE(ts.TotalPlays, 0)                                  AS TotalPlays,
    COALESCE(ts.TotalMinutes, 0)                                AS TotalMinutes,
    ts.FirstPlayed,
    ts.LastPlayed,
    ts.SkipPct,
    -- Was this track played AFTER being added to the playlist?
    CASE
        WHEN ts.LastPlayed IS NULL THEN 'Never Played'
        WHEN ts.LastPlayed >= pt.AddedDate THEN 'Played After Adding'
        ELSE 'Only Played Before Adding'
    END                                                         AS PlayRelativeToAdd
FROM dbo.PlaylistTrack pt
JOIN dbo.Playlist pl ON pt.PlaylistId = pl.PlaylistId
LEFT JOIN dbo.Track t ON pt.TrackUri = t.SpotifyUri
LEFT JOIN dbo.vw_TrackStats ts ON t.TrackId = ts.TrackId;
GO

-- Collection changes correlated with streaming
-- What you were listening to when you saved something
IF OBJECT_ID('dbo.vw_CollectionGrowth', 'V') IS NOT NULL DROP VIEW dbo.vw_CollectionGrowth;
GO
CREATE VIEW dbo.vw_CollectionGrowth AS
SELECT
    CAST(cc.ChangeTime AS DATE)                                 AS ChangeDate,
    FORMAT(cc.ChangeTime, 'yyyy-MM')                            AS YearMonth,
    cc.ChangeType,
    SUM(CASE WHEN cc.ChangeType = 'added' THEN 1 ELSE 0 END)   AS Added,
    SUM(CASE WHEN cc.ChangeType = 'removed' THEN 1 ELSE 0 END) AS Removed,
    COUNT(*)                                                    AS TotalChanges
FROM dbo.CollectionChange cc
GROUP BY CAST(cc.ChangeTime AS DATE), FORMAT(cc.ChangeTime, 'yyyy-MM'), cc.ChangeType;
GO

-- Artist discovery timeline
-- When did you first encounter each artist, and how deep did you go?
IF OBJECT_ID('dbo.vw_ArtistDiscovery', 'V') IS NOT NULL DROP VIEW dbo.vw_ArtistDiscovery;
GO
CREATE VIEW dbo.vw_ArtistDiscovery AS
SELECT
    ar.ArtistId,
    ar.ArtistName,
    s.FirstPlayed,
    s.LastPlayed,
    s.TotalPlays,
    s.TotalHours,
    s.UniqueTracksPlayed,
    s.UniqueAlbumsPlayed,
    s.DaysListened,
    s.SkipPct,
    DATEPART(YEAR, s.FirstPlayed)                               AS DiscoveryYear,
    -- Engagement level
    CASE
        WHEN s.TotalHours >= 50                  THEN 'Obsession'
        WHEN s.TotalHours >= 10                  THEN 'Heavy Rotation'
        WHEN s.TotalHours >= 2                   THEN 'Regular'
        WHEN s.TotalPlays >= 5                   THEN 'Casual'
        ELSE 'Sampled'
    END                                                         AS EngagementLevel,
    -- Recency
    CASE
        WHEN s.LastPlayed >= DATEADD(MONTH, -1, GETDATE())  THEN 'Current'
        WHEN s.LastPlayed >= DATEADD(MONTH, -3, GETDATE())  THEN 'Recent'
        WHEN s.LastPlayed >= DATEADD(YEAR, -1, GETDATE())   THEN 'Past Year'
        ELSE 'Historical'
    END                                                         AS Recency,
    -- Is this artist in the library?
    CASE WHEN la.LibraryArtistId IS NOT NULL THEN 1 ELSE 0 END AS InLibrary,
    -- What does Marquee say about engagement?
    m.Segment                                                   AS MarqueeSegment
FROM dbo.Artist ar
JOIN dbo.vw_ArtistStats s ON ar.ArtistId = s.ArtistId
LEFT JOIN dbo.LibraryArtist la ON ar.ArtistName = la.ArtistName
LEFT JOIN dbo.Marquee m ON ar.ArtistName = m.ArtistName;
GO

-- Yearly listening summary spanning all sources
IF OBJECT_ID('dbo.vw_YearlyListeningSummary', 'V') IS NOT NULL DROP VIEW dbo.vw_YearlyListeningSummary;
GO
CREATE VIEW dbo.vw_YearlyListeningSummary AS
SELECT
    DATEPART(YEAR, p.Timestamp)                                     AS [Year],
    COUNT(*)                                                        AS TotalPlays,
    CAST(SUM(CAST(p.MsPlayed AS BIGINT)) / 3600000.0 AS DECIMAL(10,1)) AS TotalHours,
    COUNT(DISTINCT CAST(p.Timestamp AS DATE))                       AS DaysActive,
    COUNT(DISTINCT CASE WHEN p.TrackId IS NOT NULL THEN p.TrackId END) AS UniqueTracksPlayed,
    COUNT(DISTINCT t.ArtistId)                                      AS UniqueArtists,
    COUNT(DISTINCT t.AlbumId)                                       AS UniqueAlbums,
    CAST(AVG(CASE WHEN p.TrackId IS NOT NULL THEN p.MsPlayed * 1.0 END) / 1000 AS DECIMAL(8,1)) AS AvgTrackSeconds,
    SUM(CASE WHEN p.Skipped = 1 THEN 1 ELSE 0 END)                 AS Skips,
    SUM(CASE WHEN p.Shuffle = 1 THEN 1 ELSE 0 END)                 AS ShufflePlays,
    SUM(CASE WHEN p.Offline = 1 THEN 1 ELSE 0 END)                 AS OfflinePlays,
    SUM(CASE WHEN p.IncognitoMode = 1 THEN 1 ELSE 0 END)           AS IncognitoPlays,
    COUNT(DISTINCT p.Platform)                                      AS PlatformsUsed
FROM dbo.Play p
LEFT JOIN dbo.Track t ON p.TrackId = t.TrackId
GROUP BY DATEPART(YEAR, p.Timestamp);
GO

-- Session activity (combining sessions with plays)
IF OBJECT_ID('dbo.vw_SessionActivity', 'V') IS NOT NULL DROP VIEW dbo.vw_SessionActivity;
GO
CREATE VIEW dbo.vw_SessionActivity AS
SELECT
    CAST(s.SessionTime AS DATE)                                 AS SessionDate,
    COUNT(DISTINCT s.SessionId)                                 AS Sessions,
    COUNT(p.PlayId)                                             AS PlaysOnDate,
    CAST(COALESCE(SUM(CAST(p.MsPlayed AS BIGINT)), 0) / 60000.0 AS DECIMAL(10,0)) AS MinutesPlayed
FROM dbo.Session s
LEFT JOIN dbo.Play p ON CAST(s.SessionTime AS DATE) = CAST(p.Timestamp AS DATE)
GROUP BY CAST(s.SessionTime AS DATE);
GO

-- Listening streaks (days in a row with plays)
IF OBJECT_ID('dbo.vw_DailyActivity', 'V') IS NOT NULL DROP VIEW dbo.vw_DailyActivity;
GO
CREATE VIEW dbo.vw_DailyActivity AS
SELECT
    CAST(p.Timestamp AS DATE)                                   AS PlayDate,
    COUNT(*)                                                    AS TotalPlays,
    CAST(SUM(CAST(p.MsPlayed AS BIGINT)) / 3600000.0 AS DECIMAL(10,2)) AS Hours,
    COUNT(DISTINCT CASE WHEN p.TrackId IS NOT NULL THEN p.TrackId END) AS UniqueTracks,
    COUNT(DISTINCT t.ArtistId)                                  AS UniqueArtists,
    MIN(p.Timestamp)                                            AS FirstPlay,
    MAX(p.Timestamp)                                            AS LastPlay,
    DATEDIFF(MINUTE, MIN(p.Timestamp), MAX(p.Timestamp))        AS ActiveMinutes
FROM dbo.Play p
LEFT JOIN dbo.Track t ON p.TrackId = t.TrackId
GROUP BY CAST(p.Timestamp AS DATE);
GO

PRINT 'All views created successfully.';
GO
