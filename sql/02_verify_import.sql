/*
    Spotify Extended Streaming History - Import Verification
    Run this after importing to verify record counts and data integrity.
*/

USE [Spotify];
GO

PRINT '=== Record Counts ===';

SELECT 'Artist'          AS [Table], COUNT(*) AS [Rows] FROM dbo.Artist
UNION ALL
SELECT 'Album',                      COUNT(*)           FROM dbo.Album
UNION ALL
SELECT 'Track',                      COUNT(*)           FROM dbo.Track
UNION ALL
SELECT 'PodcastShow',                COUNT(*)           FROM dbo.PodcastShow
UNION ALL
SELECT 'PodcastEpisode',             COUNT(*)           FROM dbo.PodcastEpisode
UNION ALL
SELECT 'Audiobook',                  COUNT(*)           FROM dbo.Audiobook
UNION ALL
SELECT 'AudiobookChapter',           COUNT(*)           FROM dbo.AudiobookChapter
UNION ALL
SELECT 'Play',                       COUNT(*)           FROM dbo.Play
ORDER BY [Table];
GO

PRINT '';
PRINT '=== Plays by Source File ===';

SELECT SourceFile, COUNT(*) AS PlayCount
FROM dbo.Play
GROUP BY SourceFile
ORDER BY SourceFile;
GO

PRINT '';
PRINT '=== Date Range ===';

SELECT
    MIN(Timestamp) AS EarliestPlay,
    MAX(Timestamp) AS LatestPlay,
    DATEDIFF(DAY, MIN(Timestamp), MAX(Timestamp)) AS DaysSpan
FROM dbo.Play;
GO

PRINT '';
PRINT '=== Top 10 Most Played Artists ===';

SELECT TOP 10
    a.ArtistName,
    COUNT(*) AS PlayCount,
    SUM(p.MsPlayed) / 3600000.0 AS TotalHours
FROM dbo.Play p
JOIN dbo.Track t ON p.TrackId = t.TrackId
JOIN dbo.Artist a ON t.ArtistId = a.ArtistId
GROUP BY a.ArtistName
ORDER BY PlayCount DESC;
GO

PRINT '';
PRINT '=== Top 10 Most Played Tracks ===';

SELECT TOP 10
    t.TrackName,
    a.ArtistName,
    COUNT(*) AS PlayCount,
    SUM(p.MsPlayed) / 60000.0 AS TotalMinutes
FROM dbo.Play p
JOIN dbo.Track t ON p.TrackId = t.TrackId
LEFT JOIN dbo.Artist a ON t.ArtistId = a.ArtistId
GROUP BY t.TrackName, a.ArtistName
ORDER BY PlayCount DESC;
GO
