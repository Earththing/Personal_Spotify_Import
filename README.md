# Spotify Personal Data Import

Import all three Spotify personal data packages into a normalized SQL Server database for analysis.

## Getting Your Data

1. Go to your [Spotify Account Privacy page](https://www.spotify.com/us/account/privacy/)
2. Request your data packages:
   - **Extended streaming history** - may take up to 30 days to prepare
   - **Account data** - can take up to 5 days
   - **Technical log data** - can take up to 30 days
3. Download each ZIP when Spotify emails you and extract them

## Prerequisites

- **SQL Server** (2017 or later) with Windows Authentication
- **PowerShell** 5.1+ (included with Windows)
- **sqlcmd** utility (included with SQL Server)

## Setup

### 1. Place Your Data

Extract each download into the `data/` directory:

```
data/
  Spotify Extended Streaming History/
    Streaming_History_Audio_*.json
    Streaming_History_Video_*.json
  Spotify Account Data/
    Userdata.json, Playlist1.json, YourLibrary.json, ...
  Spotify Technical Log Information/
    AddedToCollection.json, SessionCreation.json, ...
```

### 2. Create the Database

Run the SQL scripts in order to create the `Spotify` database and all tables:

```powershell
sqlcmd -E -i "sql\01_create_database.sql"
sqlcmd -E -i "sql\03_account_data_tables.sql"
sqlcmd -E -i "sql\04_technical_log_tables.sql"
```

For a named instance (e.g., `SQLEXPRESS`), add `-S "localhost\SQLEXPRESS"` to each command.

### 3. Import Your Data

Run each importer. Order matters - Extended Streaming History first (other importers cross-reference its tracks).

```powershell
# 1. Extended Streaming History (run first)
.\Import-SpotifyData.ps1

# 2. Account Data
.\Import-AccountData.ps1

# 3. Technical Logs
.\Import-TechnicalLogs.ps1
```

Each script accepts `-ServerInstance` and `-DataPath` parameters if your setup differs from defaults.

### 4. Create Views

```powershell
sqlcmd -E -d Spotify -i "sql\05_views.sql"
```

### 5. Verify

```powershell
sqlcmd -E -d Spotify -i "sql\02_verify_import.sql"
```

## Data Sources

### Extended Streaming History (`Import-SpotifyData.ps1`)
Your complete play-by-play listening history with full metadata. Normalized into dimension tables.

| Table | Description |
|-------|-------------|
| **Play** | Fact table - one row per stream (timestamp, duration, platform, skip/shuffle/offline flags) |
| **Artist** | Distinct artist names |
| **Album** | Albums linked to artists |
| **Track** | Tracks with Spotify URI, linked to album and artist |
| **PodcastShow** | Podcast show names |
| **PodcastEpisode** | Episodes linked to shows |
| **Audiobook** / **AudiobookChapter** | Audiobooks and chapters |

### Account Data (`Import-AccountData.ps1`)
Profile, playlists, library, search history, and Spotify's ad-targeting inferences about you.

| Table | Description |
|-------|-------------|
| **UserProfile** | Username, email, country, birthdate, creation date |
| **Follow** | Who you follow, who follows you, who you've blocked |
| **Inference** | 1,000+ ad-targeting segments Spotify assigns to you |
| **Marquee** | Spotify's artist engagement classification (Light/Moderate/Previously Active) |
| **SearchQuery** / **SearchInteraction** | Search history with what you clicked |
| **Playlist** / **PlaylistTrack** | All playlists with every track, cross-referenced to streaming Track table |
| **LibraryTrack** / **LibraryAlbum** / **LibraryArtist** | Your saved/liked items |
| **StreamingHistoryMusic** / **StreamingHistoryPodcast** | Simplified recent streaming history |

### Technical Logs (`Import-TechnicalLogs.ps1`)
Selected high-value technical events from the 170+ log file types.

| Table | Description |
|-------|-------------|
| **CollectionChange** | When tracks/albums were added or removed from your library |
| **PlaylistChange** | When tracks were added or removed from playlists |
| **RootlistChange** | When playlists were added or removed from your library |
| **ShareEvent** | What you shared and where |
| **PlaybackError** | Playback failures with error codes |
| **Session** | App session creation events |
| **AccountActivity** | Account settings page interactions |

## Views

Views combine data across all three sources for meaningful analysis.

### Streaming History Views
| View | Description |
|------|-------------|
| `vw_PlayDetail` | Every play with all dimensions resolved (track, artist, album, episode, audiobook) |
| `vw_ArtistStats` | Per-artist aggregates: plays, hours, unique tracks/albums, skip rate, date range |
| `vw_TrackStats` | Per-track aggregates: plays, minutes, skip/shuffle rates |
| `vw_AlbumStats` | Per-album aggregates |
| `vw_MonthlyListening` | Year/month breakdown with hours, unique tracks, content type split |
| `vw_ListeningHeatmap` | Day-of-week x hour-of-day play counts (for heatmap visualizations) |
| `vw_PlatformStats` | Usage by device/platform |
| `vw_SkipAnalysis` | Skip behavior by playback end reason |
| `vw_OfflineListening` | Online vs offline listening by year |
| `vw_DailyActivity` | Per-day aggregates: plays, hours, unique tracks/artists, active minutes |

### Cross-Source Views
| View | Description |
|------|-------------|
| `vw_ArtistDiscovery` | When you discovered each artist, engagement level, recency, Marquee segment, library status |
| `vw_ArtistEngagement` | Marquee segments joined with actual listening data and library/follow status |
| `vw_LibraryListeningStatus` | Which saved tracks you actually play (Active/Inactive/Dormant/Never Played) |
| `vw_PlaylistTrackActivity` | Playlist tracks matched to streaming history - which ones you actually listen to |
| `vw_CollectionGrowth` | Library adds/removes over time |
| `vw_YearlyListeningSummary` | Year-over-year stats: hours, unique tracks/artists/albums, skips, platforms |
| `vw_SessionActivity` | Sessions correlated with daily listening |
| `vw_PlaylistOverview` | Playlist summary with track counts and unique artists |
| `vw_SearchHistory` | Searches with click-through indicator |

## Example Queries

```sql
-- Yearly listening summary
SELECT * FROM vw_YearlyListeningSummary ORDER BY [Year];

-- Your top artists with discovery timeline and engagement level
SELECT TOP 20 ArtistName, DiscoveryYear, TotalHours, EngagementLevel, Recency, MarqueeSegment
FROM vw_ArtistDiscovery ORDER BY TotalHours DESC;

-- How many of your saved tracks do you actually listen to?
SELECT ListeningStatus, COUNT(*) AS Tracks
FROM vw_LibraryListeningStatus GROUP BY ListeningStatus;

-- When do you listen most? (heatmap data)
SELECT DayOfWeek, HourOfDay, TotalPlays
FROM vw_ListeningHeatmap ORDER BY DayOfWeekNum, HourOfDay;

-- Playlist tracks you never play
SELECT PlaylistName, TrackName, ArtistName
FROM vw_PlaylistTrackActivity
WHERE TotalPlays = 0 ORDER BY PlaylistName;
```

## Privacy

The `data/` directory is in `.gitignore` and will never be committed. Your streaming history, IP addresses, email, and other personal data stays local.
