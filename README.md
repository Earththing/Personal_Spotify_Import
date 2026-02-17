# Spotify Streaming History Import

Import your Spotify Extended Streaming History into a normalized SQL Server database for analysis.

## Getting Your Data

1. Go to your [Spotify Account Privacy page](https://www.spotify.com/us/account/privacy/)
2. Request your data packages:
   - **Extended streaming history** - may take up to 30 days to prepare
   - **Account data** (optional) - can take up to 5 days
   - **Technical log data** (optional) - can take up to 30 days
3. Download the ZIP when Spotify emails you and extract it

## Prerequisites

- **SQL Server** (2017 or later) with Windows Authentication
- **PowerShell** 5.1+ (included with Windows)
- **sqlcmd** utility (included with SQL Server)

## Setup

### 1. Place Your Data

Copy your extracted `Spotify Extended Streaming History` folder into the `data/` directory:

```
data/
  Spotify Extended Streaming History/
    Streaming_History_Audio_2020-2022_0.json
    Streaming_History_Audio_2022-2023_1.json
    Streaming_History_Video_2020-2023.json
    ...
```

The JSON files follow the naming pattern `Streaming_History_Audio_YYYY-YYYY_N.json` and `Streaming_History_Video_YYYY-YYYY.json`.

### 2. Create the Database

Run the SQL script to create the `Spotify` database and all tables:

```powershell
sqlcmd -E -i "sql\01_create_database.sql"
```

If you're using a named instance (e.g., `SQLEXPRESS`):

```powershell
sqlcmd -E -S "localhost\SQLEXPRESS" -i "sql\01_create_database.sql"
```

### 3. Import Your Data

```powershell
.\Import-SpotifyData.ps1
```

With a named instance:

```powershell
.\Import-SpotifyData.ps1 -ServerInstance "localhost\SQLEXPRESS"
```

Full parameter list:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `-DataPath` | `.\data\Spotify Extended Streaming History` | Path to your JSON files |
| `-ServerInstance` | `localhost` | SQL Server instance |
| `-Database` | `Spotify` | Database name |
| `-BatchSize` | `1000` | Records per batch (for tuning) |

### 4. Verify

Run the verification script to check record counts and see sample queries:

```powershell
sqlcmd -E -d Spotify -i "sql\02_verify_import.sql"
```

## Database Schema

The data is imported into a normalized star schema:

```
                    ┌──────────┐
                    │  Artist  │
                    └────┬─────┘
                         │
              ┌──────────┼──────────┐
              │          │          │
         ┌────┴───┐ ┌───┴────┐    │
         │ Album  │ │ Track  │    │
         └────────┘ └───┬────┘    │
                        │         │
┌────────────┐    ┌─────┴───┐    │
│PodcastShow │    │  Play   │◄───┘
└──────┬─────┘    └─┬───┬───┘
       │            │   │
┌──────┴───────┐    │   │    ┌───────────┐
│PodcastEpisode│◄───┘   └───►│ Audiobook │
└──────────────┘             └─────┬─────┘
                                   │
                          ┌────────┴────────┐
                          │AudiobookChapter │
                          └─────────────────┘
```

### Tables

| Table | Description |
|-------|-------------|
| **Play** | Fact table - one row per stream event (timestamp, duration, platform, etc.) |
| **Artist** | Distinct artist/band names |
| **Album** | Albums, linked to their artist |
| **Track** | Tracks with Spotify URI, linked to album and artist |
| **PodcastShow** | Podcast show names |
| **PodcastEpisode** | Episodes with Spotify URI, linked to their show |
| **Audiobook** | Audiobooks with Spotify URI |
| **AudiobookChapter** | Chapters with Spotify URI, linked to their audiobook |

### Play Table Fields

| Column | Type | Description |
|--------|------|-------------|
| PlayId | BIGINT | Auto-incrementing primary key |
| Timestamp | DATETIMEOFFSET | When the stream stopped playing (UTC) |
| Platform | NVARCHAR(100) | Device/OS used (e.g., "windows", "Android OS 12 API 31") |
| MsPlayed | INT | Milliseconds the stream was played |
| ConnCountry | CHAR(2) | Country code where stream was played |
| IpAddr | VARCHAR(45) | IP address at time of stream |
| TrackId | INT | FK to Track (null if podcast/audiobook) |
| EpisodeId | INT | FK to PodcastEpisode (null if music/audiobook) |
| AudiobookChapterId | INT | FK to AudiobookChapter (null if music/podcast) |
| ReasonStart | VARCHAR(30) | Why playback started (e.g., "trackdone", "clickrow", "fwdbtn") |
| ReasonEnd | VARCHAR(30) | Why playback ended (e.g., "trackdone", "endplay", "fwdbtn") |
| Shuffle | BIT | Whether shuffle was on |
| Skipped | BIT | Whether the track was skipped |
| Offline | BIT | Whether played offline |
| OfflineTimestamp | BIGINT | Unix timestamp for offline playback |
| IncognitoMode | BIT | Whether private session was active |
| SourceFile | NVARCHAR(200) | Original JSON filename |

## Example Queries

```sql
-- Top 10 most played artists by listen time
SELECT TOP 10
    a.ArtistName,
    COUNT(*) AS PlayCount,
    CAST(SUM(p.MsPlayed) / 3600000.0 AS DECIMAL(10,1)) AS TotalHours
FROM Play p
JOIN Track t ON p.TrackId = t.TrackId
JOIN Artist a ON t.ArtistId = a.ArtistId
GROUP BY a.ArtistName
ORDER BY TotalHours DESC;

-- Listening by year
SELECT
    YEAR(p.Timestamp) AS [Year],
    COUNT(*) AS Plays,
    CAST(SUM(p.MsPlayed) / 3600000.0 AS DECIMAL(10,1)) AS Hours
FROM Play p
GROUP BY YEAR(p.Timestamp)
ORDER BY [Year];

-- Most skipped tracks
SELECT TOP 10
    t.TrackName,
    a.ArtistName,
    COUNT(*) AS SkipCount
FROM Play p
JOIN Track t ON p.TrackId = t.TrackId
LEFT JOIN Artist a ON t.ArtistId = a.ArtistId
WHERE p.Skipped = 1
GROUP BY t.TrackName, a.ArtistName
ORDER BY SkipCount DESC;
```

## Privacy

The `data/` directory is in `.gitignore` and will never be committed. Your streaming history, IP addresses, and other personal data stays local.

## Future Data

This project is designed to grow. Additional Spotify data packages (account data, technical logs) can be added with new SQL scripts and importers as they become available.
