<#
.SYNOPSIS
    Imports Spotify Extended Streaming History JSON files into a normalized SQL Server database.

.DESCRIPTION
    Reads all JSON files from the specified data directory and imports them into the
    Spotify database with normalized dimension tables (Artist, Album, Track, PodcastShow,
    PodcastEpisode, Audiobook, AudiobookChapter) and a Play fact table.

    Requires the Spotify database and tables to already exist (run sql\01_create_database.sql first).

.PARAMETER DataPath
    Path to the folder containing Spotify Extended Streaming History JSON files.
    Defaults to .\data\Spotify Extended Streaming History

.PARAMETER ServerInstance
    SQL Server instance to connect to. Defaults to localhost (default instance).

.PARAMETER Database
    Database name. Defaults to Spotify.

.PARAMETER BatchSize
    Number of play records to insert per batch. Defaults to 1000.

.EXAMPLE
    .\Import-SpotifyData.ps1

.EXAMPLE
    .\Import-SpotifyData.ps1 -DataPath "C:\MyData\Spotify Extended Streaming History" -ServerInstance "localhost\SQLEXPRESS"
#>

[CmdletBinding()]
param(
    [string]$DataPath,
    [string]$ServerInstance = "localhost",
    [string]$Database = "Spotify",
    [int]$BatchSize = 1000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Default DataPath: look relative to script location, then current directory
if (-not $DataPath) {
    $scriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }
    $DataPath = Join-Path $scriptDir "data\Spotify Extended Streaming History"
}

# ============================================================
# SQL Server connection helper
# ============================================================
function Get-SqlConnection {
    param([string]$Server, [string]$Db)
    $conn = New-Object System.Data.SqlClient.SqlConnection
    $conn.ConnectionString = "Server=$Server;Database=$Db;Integrated Security=True;TrustServerCertificate=True"
    $conn.Open()
    return $conn
}

function Invoke-Sql {
    param(
        [System.Data.SqlClient.SqlConnection]$Connection,
        [string]$Query,
        [hashtable]$Parameters = @{}
    )
    $cmd = $Connection.CreateCommand()
    $cmd.CommandText = $Query
    $cmd.CommandTimeout = 120
    foreach ($key in $Parameters.Keys) {
        $value = $Parameters[$key]
        if ($null -eq $value) {
            [void]$cmd.Parameters.AddWithValue("@$key", [DBNull]::Value)
        } else {
            [void]$cmd.Parameters.AddWithValue("@$key", $value)
        }
    }
    return $cmd
}

function Invoke-SqlScalar {
    param(
        [System.Data.SqlClient.SqlConnection]$Connection,
        [string]$Query,
        [hashtable]$Parameters = @{}
    )
    $cmd = Invoke-Sql -Connection $Connection -Query $Query -Parameters $Parameters
    $result = $cmd.ExecuteScalar()
    $cmd.Dispose()
    return $result
}

function Invoke-SqlNonQuery {
    param(
        [System.Data.SqlClient.SqlConnection]$Connection,
        [string]$Query,
        [hashtable]$Parameters = @{}
    )
    $cmd = Invoke-Sql -Connection $Connection -Query $Query -Parameters $Parameters
    $result = $cmd.ExecuteNonQuery()
    $cmd.Dispose()
    return $result
}

# ============================================================
# In-memory lookup caches (avoid repeated DB lookups)
# ============================================================
$artistCache   = @{}   # ArtistName -> ArtistId
$albumCache    = @{}   # "ArtistId|AlbumName" -> AlbumId
$trackCache    = @{}   # SpotifyUri -> TrackId
$showCache     = @{}   # ShowName -> ShowId
$episodeCache  = @{}   # SpotifyUri -> EpisodeId
$bookCache     = @{}   # SpotifyUri -> AudiobookId
$chapterCache  = @{}   # SpotifyUri -> ChapterId

# ============================================================
# Dimension lookup/insert functions
# ============================================================
function Get-OrCreateArtist {
    param([System.Data.SqlClient.SqlConnection]$Conn, [string]$Name)
    if ($artistCache.ContainsKey($Name)) { return $artistCache[$Name] }

    $id = Invoke-SqlScalar -Connection $Conn -Query "SELECT ArtistId FROM dbo.Artist WHERE ArtistName = @n" -Parameters @{n=$Name}
    if ($null -eq $id) {
        $id = Invoke-SqlScalar -Connection $Conn -Query "INSERT INTO dbo.Artist (ArtistName) OUTPUT INSERTED.ArtistId VALUES (@n)" -Parameters @{n=$Name}
    }
    $artistCache[$Name] = $id
    return $id
}

function Get-OrCreateAlbum {
    param([System.Data.SqlClient.SqlConnection]$Conn, [string]$AlbumName, [int]$ArtistId)
    $key = "$ArtistId|$AlbumName"
    if ($albumCache.ContainsKey($key)) { return $albumCache[$key] }

    $id = Invoke-SqlScalar -Connection $Conn -Query "SELECT AlbumId FROM dbo.Album WHERE AlbumName = @n AND ArtistId = @a" -Parameters @{n=$AlbumName; a=$ArtistId}
    if ($null -eq $id) {
        $id = Invoke-SqlScalar -Connection $Conn -Query "INSERT INTO dbo.Album (AlbumName, ArtistId) OUTPUT INSERTED.AlbumId VALUES (@n, @a)" -Parameters @{n=$AlbumName; a=$ArtistId}
    }
    $albumCache[$key] = $id
    return $id
}

function Get-OrCreateTrack {
    param([System.Data.SqlClient.SqlConnection]$Conn, [string]$Uri, [string]$TrackName, $AlbumId, $ArtistId)
    if ($trackCache.ContainsKey($Uri)) { return $trackCache[$Uri] }

    $id = Invoke-SqlScalar -Connection $Conn -Query "SELECT TrackId FROM dbo.Track WHERE SpotifyUri = @u" -Parameters @{u=$Uri}
    if ($null -eq $id) {
        $id = Invoke-SqlScalar -Connection $Conn -Query "INSERT INTO dbo.Track (SpotifyUri, TrackName, AlbumId, ArtistId) OUTPUT INSERTED.TrackId VALUES (@u, @n, @alb, @art)" -Parameters @{u=$Uri; n=$TrackName; alb=$AlbumId; art=$ArtistId}
    }
    $trackCache[$Uri] = $id
    return $id
}

function Get-OrCreateShow {
    param([System.Data.SqlClient.SqlConnection]$Conn, [string]$Name)
    if ($showCache.ContainsKey($Name)) { return $showCache[$Name] }

    $id = Invoke-SqlScalar -Connection $Conn -Query "SELECT ShowId FROM dbo.PodcastShow WHERE ShowName = @n" -Parameters @{n=$Name}
    if ($null -eq $id) {
        $id = Invoke-SqlScalar -Connection $Conn -Query "INSERT INTO dbo.PodcastShow (ShowName) OUTPUT INSERTED.ShowId VALUES (@n)" -Parameters @{n=$Name}
    }
    $showCache[$Name] = $id
    return $id
}

function Get-OrCreateEpisode {
    param([System.Data.SqlClient.SqlConnection]$Conn, [string]$Uri, [string]$EpName, $ShowId)
    if ($episodeCache.ContainsKey($Uri)) { return $episodeCache[$Uri] }

    $id = Invoke-SqlScalar -Connection $Conn -Query "SELECT EpisodeId FROM dbo.PodcastEpisode WHERE SpotifyUri = @u" -Parameters @{u=$Uri}
    if ($null -eq $id) {
        $id = Invoke-SqlScalar -Connection $Conn -Query "INSERT INTO dbo.PodcastEpisode (SpotifyUri, EpisodeName, ShowId) OUTPUT INSERTED.EpisodeId VALUES (@u, @n, @s)" -Parameters @{u=$Uri; n=$EpName; s=$ShowId}
    }
    $episodeCache[$Uri] = $id
    return $id
}

function Get-OrCreateAudiobook {
    param([System.Data.SqlClient.SqlConnection]$Conn, [string]$Uri, [string]$Title)
    if ($bookCache.ContainsKey($Uri)) { return $bookCache[$Uri] }

    $id = Invoke-SqlScalar -Connection $Conn -Query "SELECT AudiobookId FROM dbo.Audiobook WHERE SpotifyUri = @u" -Parameters @{u=$Uri}
    if ($null -eq $id) {
        $id = Invoke-SqlScalar -Connection $Conn -Query "INSERT INTO dbo.Audiobook (SpotifyUri, Title) OUTPUT INSERTED.AudiobookId VALUES (@u, @t)" -Parameters @{u=$Uri; t=$Title}
    }
    $bookCache[$Uri] = $id
    return $id
}

function Get-OrCreateChapter {
    param([System.Data.SqlClient.SqlConnection]$Conn, [string]$Uri, [string]$Title, $AudiobookId)
    if ($chapterCache.ContainsKey($Uri)) { return $chapterCache[$Uri] }

    $id = Invoke-SqlScalar -Connection $Conn -Query "SELECT ChapterId FROM dbo.AudiobookChapter WHERE SpotifyUri = @u" -Parameters @{u=$Uri}
    if ($null -eq $id) {
        $id = Invoke-SqlScalar -Connection $Conn -Query "INSERT INTO dbo.AudiobookChapter (SpotifyUri, ChapterTitle, AudiobookId) OUTPUT INSERTED.ChapterId VALUES (@u, @t, @a)" -Parameters @{u=$Uri; t=$Title; a=$AudiobookId}
    }
    $chapterCache[$Uri] = $id
    return $id
}

# ============================================================
# Main Import
# ============================================================

# Validate data path
if (-not (Test-Path $DataPath)) {
    Write-Error "Data path not found: $DataPath"
    exit 1
}

$jsonFiles = Get-ChildItem -Path $DataPath -Filter "*.json" | Sort-Object Name
if ($jsonFiles.Count -eq 0) {
    Write-Error "No JSON files found in: $DataPath"
    exit 1
}

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Spotify Streaming History Import" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Server:    $ServerInstance"
Write-Host "Database:  $Database"
Write-Host "Data path: $DataPath"
Write-Host "Files:     $($jsonFiles.Count) JSON files found"
Write-Host ""

# Connect
Write-Host "Connecting to SQL Server..." -ForegroundColor Yellow
$conn = Get-SqlConnection -Server $ServerInstance -Db $Database
Write-Host "Connected." -ForegroundColor Green
Write-Host ""

# Prepare the Play INSERT statement
$playInsertSql = @"
INSERT INTO dbo.Play (
    Timestamp, Platform, MsPlayed, ConnCountry, IpAddr,
    TrackId, EpisodeId, AudiobookChapterId,
    ReasonStart, ReasonEnd, Shuffle, Skipped, Offline,
    OfflineTimestamp, IncognitoMode, SourceFile
) VALUES (
    @ts, @plat, @ms, @cc, @ip,
    @tid, @eid, @acid,
    @rs, @re, @sh, @sk, @off,
    @ots, @inc, @sf
)
"@

$totalImported = 0
$totalSkipped = 0
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

foreach ($file in $jsonFiles) {
    Write-Host "Processing: $($file.Name)" -ForegroundColor Yellow
    $fileStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

    $jsonContent = Get-Content $file.FullName -Raw -Encoding UTF8
    $records = $jsonContent | ConvertFrom-Json

    $fileImported = 0
    $fileSkipped = 0
    $recordIndex = 0

    # Use a transaction per file for performance and atomicity
    $transaction = $conn.BeginTransaction()

    try {
        foreach ($rec in $records) {
            $recordIndex++

            # Resolve dimension keys
            $trackId = $null
            $episodeId = $null
            $chapterId = $null

            # Track (music)
            if ($null -ne $rec.spotify_track_uri -and $rec.spotify_track_uri -ne "") {
                $artistId = $null
                $albumId = $null

                if ($null -ne $rec.master_metadata_album_artist_name -and $rec.master_metadata_album_artist_name -ne "") {
                    # Temporarily remove transaction for dimension inserts (use separate connection approach)
                    # Actually, we need to pass the transaction to all commands
                    $artistId = $null

                    # Check cache first
                    $artistName = $rec.master_metadata_album_artist_name
                    if ($artistCache.ContainsKey($artistName)) {
                        $artistId = $artistCache[$artistName]
                    } else {
                        $cmd = $conn.CreateCommand()
                        $cmd.Transaction = $transaction
                        $cmd.CommandText = "SELECT ArtistId FROM dbo.Artist WHERE ArtistName = @n"
                        [void]$cmd.Parameters.AddWithValue("@n", $artistName)
                        $artistId = $cmd.ExecuteScalar()
                        $cmd.Dispose()

                        if ($null -eq $artistId) {
                            $cmd = $conn.CreateCommand()
                            $cmd.Transaction = $transaction
                            $cmd.CommandText = "INSERT INTO dbo.Artist (ArtistName) OUTPUT INSERTED.ArtistId VALUES (@n)"
                            [void]$cmd.Parameters.AddWithValue("@n", $artistName)
                            $artistId = $cmd.ExecuteScalar()
                            $cmd.Dispose()
                        }
                        $artistCache[$artistName] = $artistId
                    }

                    if ($null -ne $rec.master_metadata_album_album_name -and $rec.master_metadata_album_album_name -ne "") {
                        $albumName = $rec.master_metadata_album_album_name
                        $albumKey = "$artistId|$albumName"
                        if ($albumCache.ContainsKey($albumKey)) {
                            $albumId = $albumCache[$albumKey]
                        } else {
                            $cmd = $conn.CreateCommand()
                            $cmd.Transaction = $transaction
                            $cmd.CommandText = "SELECT AlbumId FROM dbo.Album WHERE AlbumName = @n AND ArtistId = @a"
                            [void]$cmd.Parameters.AddWithValue("@n", $albumName)
                            [void]$cmd.Parameters.AddWithValue("@a", $artistId)
                            $albumId = $cmd.ExecuteScalar()
                            $cmd.Dispose()

                            if ($null -eq $albumId) {
                                $cmd = $conn.CreateCommand()
                                $cmd.Transaction = $transaction
                                $cmd.CommandText = "INSERT INTO dbo.Album (AlbumName, ArtistId) OUTPUT INSERTED.AlbumId VALUES (@n, @a)"
                                [void]$cmd.Parameters.AddWithValue("@n", $albumName)
                                [void]$cmd.Parameters.AddWithValue("@a", $artistId)
                                $albumId = $cmd.ExecuteScalar()
                                $cmd.Dispose()
                            }
                            $albumCache[$albumKey] = $albumId
                        }
                    }
                }

                $trackUri = $rec.spotify_track_uri
                if ($trackCache.ContainsKey($trackUri)) {
                    $trackId = $trackCache[$trackUri]
                } else {
                    $cmd = $conn.CreateCommand()
                    $cmd.Transaction = $transaction
                    $cmd.CommandText = "SELECT TrackId FROM dbo.Track WHERE SpotifyUri = @u"
                    [void]$cmd.Parameters.AddWithValue("@u", $trackUri)
                    $trackId = $cmd.ExecuteScalar()
                    $cmd.Dispose()

                    if ($null -eq $trackId) {
                        $trackName = if ($null -ne $rec.master_metadata_track_name) { $rec.master_metadata_track_name } else { "(Unknown)" }
                        $cmd = $conn.CreateCommand()
                        $cmd.Transaction = $transaction
                        $cmd.CommandText = "INSERT INTO dbo.Track (SpotifyUri, TrackName, AlbumId, ArtistId) OUTPUT INSERTED.TrackId VALUES (@u, @n, @alb, @art)"
                        [void]$cmd.Parameters.AddWithValue("@u", $trackUri)
                        [void]$cmd.Parameters.AddWithValue("@n", $trackName)
                        if ($null -ne $albumId) { [void]$cmd.Parameters.AddWithValue("@alb", $albumId) } else { [void]$cmd.Parameters.AddWithValue("@alb", [DBNull]::Value) }
                        if ($null -ne $artistId) { [void]$cmd.Parameters.AddWithValue("@art", $artistId) } else { [void]$cmd.Parameters.AddWithValue("@art", [DBNull]::Value) }
                        $trackId = $cmd.ExecuteScalar()
                        $cmd.Dispose()
                    }
                    $trackCache[$trackUri] = $trackId
                }
            }

            # Episode (podcast)
            if ($null -ne $rec.spotify_episode_uri -and $rec.spotify_episode_uri -ne "") {
                $showId = $null
                if ($null -ne $rec.episode_show_name -and $rec.episode_show_name -ne "") {
                    $showName = $rec.episode_show_name
                    if ($showCache.ContainsKey($showName)) {
                        $showId = $showCache[$showName]
                    } else {
                        $cmd = $conn.CreateCommand()
                        $cmd.Transaction = $transaction
                        $cmd.CommandText = "SELECT ShowId FROM dbo.PodcastShow WHERE ShowName = @n"
                        [void]$cmd.Parameters.AddWithValue("@n", $showName)
                        $showId = $cmd.ExecuteScalar()
                        $cmd.Dispose()

                        if ($null -eq $showId) {
                            $cmd = $conn.CreateCommand()
                            $cmd.Transaction = $transaction
                            $cmd.CommandText = "INSERT INTO dbo.PodcastShow (ShowName) OUTPUT INSERTED.ShowId VALUES (@n)"
                            [void]$cmd.Parameters.AddWithValue("@n", $showName)
                            $showId = $cmd.ExecuteScalar()
                            $cmd.Dispose()
                        }
                        $showCache[$showName] = $showId
                    }
                }

                $epUri = $rec.spotify_episode_uri
                if ($episodeCache.ContainsKey($epUri)) {
                    $episodeId = $episodeCache[$epUri]
                } else {
                    $cmd = $conn.CreateCommand()
                    $cmd.Transaction = $transaction
                    $cmd.CommandText = "SELECT EpisodeId FROM dbo.PodcastEpisode WHERE SpotifyUri = @u"
                    [void]$cmd.Parameters.AddWithValue("@u", $epUri)
                    $episodeId = $cmd.ExecuteScalar()
                    $cmd.Dispose()

                    if ($null -eq $episodeId) {
                        $epName = if ($null -ne $rec.episode_name) { $rec.episode_name } else { "(Unknown)" }
                        $cmd = $conn.CreateCommand()
                        $cmd.Transaction = $transaction
                        $cmd.CommandText = "INSERT INTO dbo.PodcastEpisode (SpotifyUri, EpisodeName, ShowId) OUTPUT INSERTED.EpisodeId VALUES (@u, @n, @s)"
                        [void]$cmd.Parameters.AddWithValue("@u", $epUri)
                        [void]$cmd.Parameters.AddWithValue("@n", $epName)
                        if ($null -ne $showId) { [void]$cmd.Parameters.AddWithValue("@s", $showId) } else { [void]$cmd.Parameters.AddWithValue("@s", [DBNull]::Value) }
                        $episodeId = $cmd.ExecuteScalar()
                        $cmd.Dispose()
                    }
                    $episodeCache[$epUri] = $episodeId
                }
            }

            # Audiobook chapter
            if ($null -ne $rec.audiobook_chapter_uri -and $rec.audiobook_chapter_uri -ne "") {
                $audiobookId = $null
                if ($null -ne $rec.audiobook_uri -and $rec.audiobook_uri -ne "") {
                    $abUri = $rec.audiobook_uri
                    if ($bookCache.ContainsKey($abUri)) {
                        $audiobookId = $bookCache[$abUri]
                    } else {
                        $cmd = $conn.CreateCommand()
                        $cmd.Transaction = $transaction
                        $cmd.CommandText = "SELECT AudiobookId FROM dbo.Audiobook WHERE SpotifyUri = @u"
                        [void]$cmd.Parameters.AddWithValue("@u", $abUri)
                        $audiobookId = $cmd.ExecuteScalar()
                        $cmd.Dispose()

                        if ($null -eq $audiobookId) {
                            $abTitle = if ($null -ne $rec.audiobook_title) { $rec.audiobook_title } else { "(Unknown)" }
                            $cmd = $conn.CreateCommand()
                            $cmd.Transaction = $transaction
                            $cmd.CommandText = "INSERT INTO dbo.Audiobook (SpotifyUri, Title) OUTPUT INSERTED.AudiobookId VALUES (@u, @t)"
                            [void]$cmd.Parameters.AddWithValue("@u", $abUri)
                            [void]$cmd.Parameters.AddWithValue("@t", $abTitle)
                            $audiobookId = $cmd.ExecuteScalar()
                            $cmd.Dispose()
                        }
                        $bookCache[$abUri] = $audiobookId
                    }
                }

                $chUri = $rec.audiobook_chapter_uri
                if ($chapterCache.ContainsKey($chUri)) {
                    $chapterId = $chapterCache[$chUri]
                } else {
                    $cmd = $conn.CreateCommand()
                    $cmd.Transaction = $transaction
                    $cmd.CommandText = "SELECT ChapterId FROM dbo.AudiobookChapter WHERE SpotifyUri = @u"
                    [void]$cmd.Parameters.AddWithValue("@u", $chUri)
                    $chapterId = $cmd.ExecuteScalar()
                    $cmd.Dispose()

                    if ($null -eq $chapterId) {
                        $chTitle = if ($null -ne $rec.audiobook_chapter_title) { $rec.audiobook_chapter_title } else { "(Unknown)" }
                        $cmd = $conn.CreateCommand()
                        $cmd.Transaction = $transaction
                        $cmd.CommandText = "INSERT INTO dbo.AudiobookChapter (SpotifyUri, ChapterTitle, AudiobookId) OUTPUT INSERTED.ChapterId VALUES (@u, @t, @a)"
                        [void]$cmd.Parameters.AddWithValue("@u", $chUri)
                        [void]$cmd.Parameters.AddWithValue("@t", $chTitle)
                        if ($null -ne $audiobookId) { [void]$cmd.Parameters.AddWithValue("@a", $audiobookId) } else { [void]$cmd.Parameters.AddWithValue("@a", [DBNull]::Value) }
                        $chapterId = $cmd.ExecuteScalar()
                        $cmd.Dispose()
                    }
                    $chapterCache[$chUri] = $chapterId
                }
            }

            # Insert Play record
            $cmd = $conn.CreateCommand()
            $cmd.Transaction = $transaction
            $cmd.CommandText = $playInsertSql

            # Parse timestamp
            $ts = [DateTimeOffset]::Parse($rec.ts, [System.Globalization.CultureInfo]::InvariantCulture)
            [void]$cmd.Parameters.AddWithValue("@ts", $ts)

            # Platform
            if ($null -ne $rec.platform) { [void]$cmd.Parameters.AddWithValue("@plat", $rec.platform) }
            else { [void]$cmd.Parameters.AddWithValue("@plat", [DBNull]::Value) }

            [void]$cmd.Parameters.AddWithValue("@ms", $rec.ms_played)

            if ($null -ne $rec.conn_country -and $rec.conn_country -ne "") { [void]$cmd.Parameters.AddWithValue("@cc", $rec.conn_country) }
            else { [void]$cmd.Parameters.AddWithValue("@cc", [DBNull]::Value) }

            if ($null -ne $rec.ip_addr -and $rec.ip_addr -ne "") { [void]$cmd.Parameters.AddWithValue("@ip", $rec.ip_addr) }
            else { [void]$cmd.Parameters.AddWithValue("@ip", [DBNull]::Value) }

            if ($null -ne $trackId) { [void]$cmd.Parameters.AddWithValue("@tid", $trackId) }
            else { [void]$cmd.Parameters.AddWithValue("@tid", [DBNull]::Value) }

            if ($null -ne $episodeId) { [void]$cmd.Parameters.AddWithValue("@eid", $episodeId) }
            else { [void]$cmd.Parameters.AddWithValue("@eid", [DBNull]::Value) }

            if ($null -ne $chapterId) { [void]$cmd.Parameters.AddWithValue("@acid", $chapterId) }
            else { [void]$cmd.Parameters.AddWithValue("@acid", [DBNull]::Value) }

            if ($null -ne $rec.reason_start -and $rec.reason_start -ne "") { [void]$cmd.Parameters.AddWithValue("@rs", $rec.reason_start) }
            else { [void]$cmd.Parameters.AddWithValue("@rs", [DBNull]::Value) }

            if ($null -ne $rec.reason_end -and $rec.reason_end -ne "") { [void]$cmd.Parameters.AddWithValue("@re", $rec.reason_end) }
            else { [void]$cmd.Parameters.AddWithValue("@re", [DBNull]::Value) }

            # Booleans (can be null, true, or false)
            if ($null -ne $rec.shuffle) { [void]$cmd.Parameters.AddWithValue("@sh", [bool]$rec.shuffle) }
            else { [void]$cmd.Parameters.AddWithValue("@sh", [DBNull]::Value) }

            if ($null -ne $rec.skipped) { [void]$cmd.Parameters.AddWithValue("@sk", [bool]$rec.skipped) }
            else { [void]$cmd.Parameters.AddWithValue("@sk", [DBNull]::Value) }

            if ($null -ne $rec.offline) { [void]$cmd.Parameters.AddWithValue("@off", [bool]$rec.offline) }
            else { [void]$cmd.Parameters.AddWithValue("@off", [DBNull]::Value) }

            if ($null -ne $rec.offline_timestamp -and $rec.offline_timestamp -ne 0) { [void]$cmd.Parameters.AddWithValue("@ots", [long]$rec.offline_timestamp) }
            else { [void]$cmd.Parameters.AddWithValue("@ots", [DBNull]::Value) }

            if ($null -ne $rec.incognito_mode) { [void]$cmd.Parameters.AddWithValue("@inc", [bool]$rec.incognito_mode) }
            else { [void]$cmd.Parameters.AddWithValue("@inc", [DBNull]::Value) }

            [void]$cmd.Parameters.AddWithValue("@sf", $file.Name)

            $cmd.ExecuteNonQuery() | Out-Null
            $cmd.Dispose()

            $fileImported++

            # Progress every 5000 records
            if ($recordIndex % 5000 -eq 0) {
                Write-Host "  ... $recordIndex / $($records.Count) records" -ForegroundColor Gray
            }
        }

        $transaction.Commit()
    }
    catch {
        Write-Host "  ERROR at record $recordIndex : $_" -ForegroundColor Red
        try { $transaction.Rollback() } catch { }
        throw
    }

    $fileStopwatch.Stop()
    $totalImported += $fileImported
    Write-Host "  Done: $fileImported records in $([math]::Round($fileStopwatch.Elapsed.TotalSeconds, 1))s" -ForegroundColor Green
}

$stopwatch.Stop()
$conn.Close()
$conn.Dispose()

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Import Complete" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Total records imported: $totalImported"
Write-Host "Total time: $([math]::Round($stopwatch.Elapsed.TotalMinutes, 1)) minutes"
Write-Host ""
Write-Host "Dimension summary (from cache):"
Write-Host "  Artists:           $($artistCache.Count)"
Write-Host "  Albums:            $($albumCache.Count)"
Write-Host "  Tracks:            $($trackCache.Count)"
Write-Host "  Podcast Shows:     $($showCache.Count)"
Write-Host "  Podcast Episodes:  $($episodeCache.Count)"
Write-Host "  Audiobooks:        $($bookCache.Count)"
Write-Host "  Audiobook Chapters:$($chapterCache.Count)"
