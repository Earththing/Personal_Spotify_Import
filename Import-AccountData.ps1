<#
.SYNOPSIS
    Imports Spotify Account Data JSON files into the Spotify SQL Server database.

.PARAMETER DataPath
    Path to the Spotify Account Data folder.

.PARAMETER ServerInstance
    SQL Server instance. Defaults to localhost.

.PARAMETER Database
    Database name. Defaults to Spotify.
#>

[CmdletBinding()]
param(
    [string]$DataPath,
    [string]$ServerInstance = "localhost",
    [string]$Database = "Spotify"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not $DataPath) {
    $scriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }
    $DataPath = Join-Path $scriptDir "data\Spotify Account Data"
}

if (-not (Test-Path $DataPath)) {
    Write-Error "Data path not found: $DataPath"
    exit 1
}

# ============================================================
# SQL helpers
# ============================================================
function Get-SqlConnection {
    param([string]$Server, [string]$Db)
    $conn = New-Object System.Data.SqlClient.SqlConnection
    $conn.ConnectionString = "Server=$Server;Database=$Db;Integrated Security=True;TrustServerCertificate=True"
    $conn.Open()
    return $conn
}

function Invoke-SqlCmd {
    param(
        [System.Data.SqlClient.SqlConnection]$Conn,
        [System.Data.SqlClient.SqlTransaction]$Tx,
        [string]$Query,
        [hashtable]$Params = @{}
    )
    $cmd = $Conn.CreateCommand()
    if ($Tx) { $cmd.Transaction = $Tx }
    $cmd.CommandText = $Query
    $cmd.CommandTimeout = 120
    foreach ($key in $Params.Keys) {
        $val = $Params[$key]
        if ($null -eq $val -or ($val -is [string] -and $val -eq "")) {
            [void]$cmd.Parameters.AddWithValue("@$key", [DBNull]::Value)
        } else {
            [void]$cmd.Parameters.AddWithValue("@$key", $val)
        }
    }
    return $cmd
}

function SqlExec {
    param($Conn, $Tx, [string]$Query, [hashtable]$Params = @{})
    $cmd = Invoke-SqlCmd -Conn $Conn -Tx $Tx -Query $Query -Params $Params
    $cmd.ExecuteNonQuery() | Out-Null
    $cmd.Dispose()
}

function SqlScalar {
    param($Conn, $Tx, [string]$Query, [hashtable]$Params = @{})
    $cmd = Invoke-SqlCmd -Conn $Conn -Tx $Tx -Query $Query -Params $Params
    $result = $cmd.ExecuteScalar()
    $cmd.Dispose()
    return $result
}

# ============================================================
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Spotify Account Data Import" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Server:    $ServerInstance"
Write-Host "Database:  $Database"
Write-Host "Data path: $DataPath"
Write-Host ""

$conn = Get-SqlConnection -Server $ServerInstance -Db $Database
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

# ============================================================
# 1. UserProfile (Userdata.json + Identity.json)
# ============================================================
Write-Host "Importing UserProfile..." -ForegroundColor Yellow

$userFile = Join-Path $DataPath "Userdata.json"
$identFile = Join-Path $DataPath "Identity.json"

if (Test-Path $userFile) {
    $userData = Get-Content $userFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $identData = if (Test-Path $identFile) { Get-Content $identFile -Raw -Encoding UTF8 | ConvertFrom-Json } else { $null }

    # Check if already imported
    $existing = SqlScalar -Conn $conn -Tx $null -Query "SELECT COUNT(*) FROM dbo.UserProfile"
    if ($existing -eq 0) {
        SqlExec -Conn $conn -Tx $null -Query @"
INSERT INTO dbo.UserProfile (Username, Email, Country, Birthdate, Gender, CreationTime, DisplayName, ImageUrl, TasteMaker, Verified)
VALUES (@u, @e, @c, @b, @g, @ct, @dn, @img, @tm, @v)
"@ -Params @{
            u = $userData.username
            e = $userData.email
            c = $userData.country
            b = if ($userData.birthdate) { [DateTime]::Parse($userData.birthdate) } else { $null }
            g = $userData.gender
            ct = if ($userData.creationTime) { [DateTime]::Parse($userData.creationTime) } else { $null }
            dn = if ($identData) { $identData.displayName } else { $null }
            img = if ($identData) { $identData.imageUrl } else { $null }
            tm = if ($identData -and $null -ne $identData.tasteMaker) { [bool]$identData.tasteMaker } else { $null }
            v = if ($identData -and $null -ne $identData.verified) { [bool]$identData.verified } else { $null }
        }
        Write-Host "  Done: 1 record" -ForegroundColor Green
    } else {
        Write-Host "  Skipped (already exists)" -ForegroundColor Gray
    }
}

# ============================================================
# 2. Follow
# ============================================================
Write-Host "Importing Follow..." -ForegroundColor Yellow
$followFile = Join-Path $DataPath "Follow.json"
if (Test-Path $followFile) {
    $followData = Get-Content $followFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $tx = $conn.BeginTransaction()
    $count = 0
    try {
        foreach ($user in $followData.userIsFollowing) {
            SqlExec -Conn $conn -Tx $tx -Query "INSERT INTO dbo.Follow (Relationship, Username) VALUES ('following', @u)" -Params @{u=$user}
            $count++
        }
        foreach ($user in $followData.userIsFollowedBy) {
            SqlExec -Conn $conn -Tx $tx -Query "INSERT INTO dbo.Follow (Relationship, Username) VALUES ('follower', @u)" -Params @{u=$user}
            $count++
        }
        if ($followData.userIsBlocking) {
            foreach ($user in $followData.userIsBlocking) {
                SqlExec -Conn $conn -Tx $tx -Query "INSERT INTO dbo.Follow (Relationship, Username) VALUES ('blocking', @u)" -Params @{u=$user}
                $count++
            }
        }
        $tx.Commit()
        Write-Host "  Done: $count records" -ForegroundColor Green
    } catch { $tx.Rollback(); throw }
}

# ============================================================
# 3. Inferences
# ============================================================
Write-Host "Importing Inferences..." -ForegroundColor Yellow
$infFile = Join-Path $DataPath "Inferences.json"
if (Test-Path $infFile) {
    $infData = Get-Content $infFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $tx = $conn.BeginTransaction()
    $count = 0
    try {
        foreach ($inf in $infData.inferences) {
            SqlExec -Conn $conn -Tx $tx -Query "INSERT INTO dbo.Inference (InferenceValue) VALUES (@v)" -Params @{v=$inf}
            $count++
        }
        $tx.Commit()
        Write-Host "  Done: $count records" -ForegroundColor Green
    } catch { $tx.Rollback(); throw }
}

# ============================================================
# 4. Marquee
# ============================================================
Write-Host "Importing Marquee..." -ForegroundColor Yellow
$marqFile = Join-Path $DataPath "Marquee.json"
if (Test-Path $marqFile) {
    $marqData = Get-Content $marqFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $tx = $conn.BeginTransaction()
    $count = 0
    try {
        foreach ($m in $marqData) {
            SqlExec -Conn $conn -Tx $tx -Query "INSERT INTO dbo.Marquee (ArtistName, Segment) VALUES (@a, @s)" -Params @{a=$m.artistName; s=$m.segment}
            $count++
        }
        $tx.Commit()
        Write-Host "  Done: $count records" -ForegroundColor Green
    } catch { $tx.Rollback(); throw }
}

# ============================================================
# 5. Search Queries
# ============================================================
Write-Host "Importing SearchQueries..." -ForegroundColor Yellow
$sqFile = Join-Path $DataPath "SearchQueries.json"
if (Test-Path $sqFile) {
    $sqData = Get-Content $sqFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $tx = $conn.BeginTransaction()
    $count = 0
    try {
        foreach ($sq in $sqData) {
            # Parse timestamp - strip timezone name suffix like "[UTC]"
            $timeStr = $sq.searchTime -replace '\[.*?\]$', ''
            $searchTime = [DateTimeOffset]::Parse($timeStr, [System.Globalization.CultureInfo]::InvariantCulture)

            $sqId = SqlScalar -Conn $conn -Tx $tx -Query @"
INSERT INTO dbo.SearchQuery (Platform, SearchTime, SearchQueryText) OUTPUT INSERTED.SearchQueryId
VALUES (@p, @t, @q)
"@ -Params @{p=$sq.platform; t=$searchTime; q=$sq.searchQuery}

            if ($sq.searchInteractionURIs -and $sq.searchInteractionURIs.Count -gt 0) {
                foreach ($uri in $sq.searchInteractionURIs) {
                    SqlExec -Conn $conn -Tx $tx -Query "INSERT INTO dbo.SearchInteraction (SearchQueryId, InteractionUri) VALUES (@id, @u)" -Params @{id=$sqId; u=$uri}
                }
            }
            $count++
        }
        $tx.Commit()
        Write-Host "  Done: $count records" -ForegroundColor Green
    } catch { $tx.Rollback(); throw }
}

# ============================================================
# 6. Playlists (Playlist1-N.json)
# ============================================================
Write-Host "Importing Playlists..." -ForegroundColor Yellow
$playlistFiles = Get-ChildItem -Path $DataPath -Filter "Playlist*.json" | Sort-Object Name
$totalPlaylists = 0
$totalPlaylistTracks = 0

# Build a lookup from track URI -> TrackId for cross-referencing
$trackLookup = @{}
$trackCmd = $conn.CreateCommand()
$trackCmd.CommandText = "SELECT SpotifyUri, TrackId FROM dbo.Track"
$reader = $trackCmd.ExecuteReader()
while ($reader.Read()) {
    $trackLookup[$reader.GetString(0)] = $reader.GetInt32(1)
}
$reader.Close()
$trackCmd.Dispose()

foreach ($plFile in $playlistFiles) {
    $plData = Get-Content $plFile.FullName -Raw -Encoding UTF8 | ConvertFrom-Json
    $tx = $conn.BeginTransaction()
    try {
        foreach ($pl in $plData.playlists) {
            $plId = SqlScalar -Conn $conn -Tx $tx -Query @"
INSERT INTO dbo.Playlist (PlaylistName, LastModifiedDate, NumberOfFollowers, SourceFile)
OUTPUT INSERTED.PlaylistId
VALUES (@n, @d, @f, @sf)
"@ -Params @{
                n = $pl.name
                d = if ($pl.lastModifiedDate) { [DateTime]::Parse($pl.lastModifiedDate) } else { $null }
                f = $pl.numberOfFollowers
                sf = $plFile.Name
            }

            # Collaborators
            if ($pl.collaborators -and $pl.collaborators.Count -gt 0) {
                foreach ($collab in $pl.collaborators) {
                    SqlExec -Conn $conn -Tx $tx -Query "INSERT INTO dbo.PlaylistCollaborator (PlaylistId, Username) VALUES (@pid, @u)" -Params @{pid=$plId; u=$collab}
                }
            }

            # Tracks
            if ($pl.items) {
                foreach ($item in $pl.items) {
                    # Some playlist items may not have a track property (local files, removed content)
                    $trackInfo = $null
                    try { $trackInfo = $item.track } catch { }
                    if (-not $trackInfo) { continue }
                    $trackId = $null
                    if ($trackInfo.trackUri -and $trackLookup.ContainsKey($trackInfo.trackUri)) {
                        $trackId = $trackLookup[$trackInfo.trackUri]
                    }
                    SqlExec -Conn $conn -Tx $tx -Query @"
INSERT INTO dbo.PlaylistTrack (PlaylistId, TrackUri, TrackName, ArtistName, AlbumName, AddedDate, TrackId)
VALUES (@pid, @uri, @tn, @an, @aln, @ad, @tid)
"@ -Params @{
                        pid = $plId
                        uri = $trackInfo.trackUri
                        tn = $trackInfo.trackName
                        an = $trackInfo.artistName
                        aln = $trackInfo.albumName
                        ad = if ($item.addedDate) { [DateTime]::Parse($item.addedDate) } else { $null }
                        tid = $trackId
                    }
                    $totalPlaylistTracks++
                }
            }
            $totalPlaylists++
        }
        $tx.Commit()
    } catch { $tx.Rollback(); throw }
}
Write-Host "  Done: $totalPlaylists playlists, $totalPlaylistTracks tracks" -ForegroundColor Green

# ============================================================
# 7. Library (YourLibrary.json)
# ============================================================
Write-Host "Importing Library..." -ForegroundColor Yellow
$libFile = Join-Path $DataPath "YourLibrary.json"
if (Test-Path $libFile) {
    $libData = Get-Content $libFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $tx = $conn.BeginTransaction()
    $trackCount = 0; $albumCount = 0; $artistCount = 0
    try {
        # Tracks
        if ($libData.tracks) {
            foreach ($tr in $libData.tracks) {
                $trackId = $null
                if ($tr.uri -and $trackLookup.ContainsKey($tr.uri)) {
                    $trackId = $trackLookup[$tr.uri]
                }
                SqlExec -Conn $conn -Tx $tx -Query @"
INSERT INTO dbo.LibraryTrack (TrackUri, TrackName, ArtistName, AlbumName, TrackId)
VALUES (@uri, @tn, @an, @aln, @tid)
"@ -Params @{uri=$tr.uri; tn=$tr.track; an=$tr.artist; aln=$tr.album; tid=$trackId}
                $trackCount++
            }
        }
        # Albums
        if ($libData.albums) {
            foreach ($al in $libData.albums) {
                SqlExec -Conn $conn -Tx $tx -Query "INSERT INTO dbo.LibraryAlbum (AlbumUri, AlbumName, ArtistName) VALUES (@uri, @n, @a)" -Params @{uri=$al.uri; n=$al.album; a=$al.artist}
                $albumCount++
            }
        }
        # Artists
        if ($libData.artists) {
            foreach ($ar in $libData.artists) {
                SqlExec -Conn $conn -Tx $tx -Query "INSERT INTO dbo.LibraryArtist (ArtistUri, ArtistName) VALUES (@uri, @n)" -Params @{uri=$ar.uri; n=$ar.name}
                $artistCount++
            }
        }
        $tx.Commit()
        Write-Host "  Done: $trackCount tracks, $albumCount albums, $artistCount artists" -ForegroundColor Green
    } catch { $tx.Rollback(); throw }
}

# ============================================================
# 8. Streaming History (Account Data version)
# ============================================================
Write-Host "Importing StreamingHistory (Music)..." -ForegroundColor Yellow
$shMusicFiles = Get-ChildItem -Path $DataPath -Filter "StreamingHistory_music_*.json" | Sort-Object Name
$shMusicTotal = 0
foreach ($shFile in $shMusicFiles) {
    $shData = Get-Content $shFile.FullName -Raw -Encoding UTF8 | ConvertFrom-Json
    $tx = $conn.BeginTransaction()
    $count = 0
    try {
        foreach ($rec in $shData) {
            $endTime = [DateTime]::ParseExact($rec.endTime, "yyyy-MM-dd HH:mm", [System.Globalization.CultureInfo]::InvariantCulture)
            SqlExec -Conn $conn -Tx $tx -Query @"
INSERT INTO dbo.StreamingHistoryMusic (EndTime, ArtistName, TrackName, MsPlayed, SourceFile)
VALUES (@t, @a, @tn, @ms, @sf)
"@ -Params @{t=$endTime; a=$rec.artistName; tn=$rec.trackName; ms=$rec.msPlayed; sf=$shFile.Name}
            $count++
        }
        $tx.Commit()
        $shMusicTotal += $count
        Write-Host "  $($shFile.Name): $count records" -ForegroundColor Gray
    } catch { $tx.Rollback(); throw }
}
Write-Host "  Done: $shMusicTotal total music records" -ForegroundColor Green

Write-Host "Importing StreamingHistory (Podcast)..." -ForegroundColor Yellow
$shPodFiles = Get-ChildItem -Path $DataPath -Filter "StreamingHistory_podcast_*.json" | Sort-Object Name
$shPodTotal = 0
foreach ($shFile in $shPodFiles) {
    $shData = Get-Content $shFile.FullName -Raw -Encoding UTF8 | ConvertFrom-Json
    $tx = $conn.BeginTransaction()
    $count = 0
    try {
        foreach ($rec in $shData) {
            $endTime = [DateTime]::ParseExact($rec.endTime, "yyyy-MM-dd HH:mm", [System.Globalization.CultureInfo]::InvariantCulture)
            SqlExec -Conn $conn -Tx $tx -Query @"
INSERT INTO dbo.StreamingHistoryPodcast (EndTime, PodcastName, EpisodeName, MsPlayed, SourceFile)
VALUES (@t, @p, @e, @ms, @sf)
"@ -Params @{t=$endTime; p=$rec.podcastName; e=$rec.episodeName; ms=$rec.msPlayed; sf=$shFile.Name}
            $count++
        }
        $tx.Commit()
        $shPodTotal += $count
    } catch { $tx.Rollback(); throw }
}
Write-Host "  Done: $shPodTotal total podcast records" -ForegroundColor Green

$stopwatch.Stop()
$conn.Close()
$conn.Dispose()

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Account Data Import Complete" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Time: $([math]::Round($stopwatch.Elapsed.TotalSeconds, 1))s"
Write-Host "  UserProfile:       1"
Write-Host "  Follow:            (see above)"
Write-Host "  Inferences:        (see above)"
Write-Host "  Marquee:           (see above)"
Write-Host "  Search Queries:    (see above)"
Write-Host "  Playlists:         $totalPlaylists playlists, $totalPlaylistTracks tracks"
Write-Host "  Library:           $trackCount tracks, $albumCount albums, $artistCount artists"
Write-Host "  Streaming Music:   $shMusicTotal"
Write-Host "  Streaming Podcast: $shPodTotal"
