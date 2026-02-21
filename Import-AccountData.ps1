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

# ============================================================
# 9. DuoNewFamily
# ============================================================
Write-Host "Importing DuoNewFamily..." -ForegroundColor Yellow
$duoFile = Join-Path $DataPath "DuoNewFamily.json"
if (Test-Path $duoFile) {
    $duoData = Get-Content $duoFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $existing = SqlScalar -Conn $conn -Tx $null -Query "SELECT COUNT(*) FROM dbo.DuoFamily"
    if ($existing -eq 0) {
        SqlExec -Conn $conn -Tx $null -Query "INSERT INTO dbo.DuoFamily ([Address]) VALUES (@a)" -Params @{a=$duoData.address}
        Write-Host "  Done: 1 record" -ForegroundColor Green
    } else {
        Write-Host "  Skipped (already exists)" -ForegroundColor Gray
    }
}

# ============================================================
# 10. Identifiers
# ============================================================
Write-Host "Importing Identifiers..." -ForegroundColor Yellow
$idFile = Join-Path $DataPath "Identifiers.json"
if (Test-Path $idFile) {
    $idData = Get-Content $idFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $existing = SqlScalar -Conn $conn -Tx $null -Query "SELECT COUNT(*) FROM dbo.Identifier"
    if ($existing -eq 0) {
        SqlExec -Conn $conn -Tx $null -Query "INSERT INTO dbo.Identifier (IdentifierType, IdentifierValue) VALUES (@t, @v)" -Params @{t=$idData.identifierType; v=$idData.identifierValue}
        Write-Host "  Done: 1 record" -ForegroundColor Green
    } else {
        Write-Host "  Skipped (already exists)" -ForegroundColor Gray
    }
}

# ============================================================
# 11. Payments
# ============================================================
Write-Host "Importing Payments..." -ForegroundColor Yellow
$payFile = Join-Path $DataPath "Payments.json"
if (Test-Path $payFile) {
    $payData = Get-Content $payFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $existing = SqlScalar -Conn $conn -Tx $null -Query "SELECT COUNT(*) FROM dbo.Payment"
    if ($existing -eq 0) {
        $creDate = if ($payData.creation_date) { [DateTime]::Parse($payData.creation_date) } else { $null }
        SqlExec -Conn $conn -Tx $null -Query @"
INSERT INTO dbo.Payment (PaymentMethod, CreationDate, Country, PostalCode)
VALUES (@pm, @cd, @c, @pc)
"@ -Params @{pm=$payData.payment_method; cd=$creDate; c=$payData.country; pc=$payData.postal_code}
        Write-Host "  Done: 1 record" -ForegroundColor Green
    } else {
        Write-Host "  Skipped (already exists)" -ForegroundColor Gray
    }
}

# ============================================================
# 12. UserAddress (Scala Map format - needs custom parser)
# ============================================================
Write-Host "Importing UserAddress..." -ForegroundColor Yellow
$uaFile = Join-Path $DataPath "UserAddress.json"
if (Test-Path $uaFile) {
    $rawContent = Get-Content $uaFile -Raw -Encoding UTF8
    $existing = SqlScalar -Conn $conn -Tx $null -Query "SELECT COUNT(*) FROM dbo.UserAddress"
    if ($existing -eq 0) {
        # Parse Scala List(Map(...), Map(...)) format
        $mapMatches = [regex]::Matches($rawContent, 'Map\(([^)]+)\)')
        $tx = $conn.BeginTransaction()
        $uaCount = 0
        try {
            foreach ($mapMatch in $mapMatches) {
                $pairs = @{}
                $kvMatches = [regex]::Matches($mapMatch.Groups[1].Value, '(\w+)\s*->\s*([^,]+)')
                foreach ($kv in $kvMatches) {
                    $pairs[$kv.Groups[1].Value.Trim()] = $kv.Groups[2].Value.Trim()
                }
                SqlExec -Conn $conn -Tx $tx -Query @"
INSERT INTO dbo.UserAddress (Street, City, [State], PostalCodeShort, PostalCodeExtra)
VALUES (@st, @ci, @s, @ps, @pe)
"@ -Params @{
                    st = if ($pairs.ContainsKey('street')) { $pairs['street'] } else { $null }
                    ci = if ($pairs.ContainsKey('city')) { $pairs['city'] } else { $null }
                    s  = if ($pairs.ContainsKey('state')) { $pairs['state'] } else { $null }
                    ps = if ($pairs.ContainsKey('postal_code_short')) { $pairs['postal_code_short'] } else { $null }
                    pe = if ($pairs.ContainsKey('postal_code_extra')) { $pairs['postal_code_extra'] } else { $null }
                }
                $uaCount++
            }
            $tx.Commit()
            Write-Host "  Done: $uaCount records" -ForegroundColor Green
        } catch { $tx.Rollback(); throw }
    } else {
        Write-Host "  Skipped (already exists)" -ForegroundColor Gray
    }
}

# ============================================================
# 13. UserPrompts
# ============================================================
Write-Host "Importing UserPrompts..." -ForegroundColor Yellow
$upFile = Join-Path $DataPath "UserPrompts.json"
if (Test-Path $upFile) {
    $upData = Get-Content $upFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $existing = SqlScalar -Conn $conn -Tx $null -Query "SELECT COUNT(*) FROM dbo.UserPrompt"
    if ($existing -eq 0) {
        $ts = $null
        if ($upData.created_timestamp) {
            try { $ts = [DateTimeOffset]::Parse($upData.created_timestamp, [System.Globalization.CultureInfo]::InvariantCulture) } catch { }
        }
        SqlExec -Conn $conn -Tx $null -Query "INSERT INTO dbo.UserPrompt (CreatedTimestamp, [Message]) VALUES (@ts, @m)" -Params @{ts=$ts; m=$upData.message}
        Write-Host "  Done: 1 record" -ForegroundColor Green
    } else {
        Write-Host "  Skipped (already exists)" -ForegroundColor Gray
    }
}

# ============================================================
# 14. UserFestivalsDataForSAR
# ============================================================
Write-Host "Importing UserFestivals..." -ForegroundColor Yellow
$ufFile = Join-Path $DataPath "UserFestivalsDataForSAR.json"
if (Test-Path $ufFile) {
    $ufData = Get-Content $ufFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $existing = SqlScalar -Conn $conn -Tx $null -Query "SELECT COUNT(*) FROM dbo.UserFestival"
    if ($existing -eq 0) {
        $topArtistsJson = "[]"
        if ($ufData.topArtists -and $ufData.topArtists.Count -gt 0) {
            $names = @($ufData.topArtists | ForEach-Object { $_.name })
            $topArtistsJson = ($names | ConvertTo-Json -Compress)
        }
        $topDiscJson = "[]"
        if ($ufData.topDiscoveryArtists -and $ufData.topDiscoveryArtists.Count -gt 0) {
            $names = @($ufData.topDiscoveryArtists | ForEach-Object { $_.name })
            $topDiscJson = ($names | ConvertTo-Json -Compress)
        }
        SqlExec -Conn $conn -Tx $null -Query @"
INSERT INTO dbo.UserFestival (FestivalId, UserId, TotalArtistsMatched, MatchPercentile, FestivalPersona, TopArtists, TopDiscoveryArtists)
VALUES (@fi, @ui, @tam, @mp, @fp, @ta, @tda)
"@ -Params @{
            fi=$ufData.festivalId; ui=$ufData.userId
            tam=$ufData.totalArtistsMatched; mp=$ufData.userLineupMatchPercentile
            fp=$ufData.festivalPersona; ta=$topArtistsJson; tda=$topDiscJson
        }
        Write-Host "  Done: 1 record" -ForegroundColor Green
    } else {
        Write-Host "  Skipped (already exists)" -ForegroundColor Gray
    }
}

# ============================================================
# 15. MessageData (in-app chat)
# ============================================================
Write-Host "Importing MessageData..." -ForegroundColor Yellow
$msgFile = Join-Path $DataPath "MessageData.json"
if (Test-Path $msgFile) {
    $msgRaw = Get-Content $msgFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $existing = SqlScalar -Conn $conn -Tx $null -Query "SELECT COUNT(*) FROM dbo.ChatConversation"
    if ($existing -eq 0) {
        $tx = $conn.BeginTransaction()
        $chatCount = 0; $msgCount = 0
        try {
            $msgRaw | Get-Member -MemberType NoteProperty | ForEach-Object {
                $chatUri = $_.Name
                $chat = $msgRaw.$chatUri
                $membersJson = ($chat.members | ConvertTo-Json -Compress)
                $convId = SqlScalar -Conn $conn -Tx $tx -Query @"
INSERT INTO dbo.ChatConversation (ChatUri, Members) OUTPUT INSERTED.ChatConversationId VALUES (@u, @m)
"@ -Params @{u=$chatUri; m=$membersJson}
                $chatCount++

                foreach ($msg in $chat.messages) {
                    $msgTime = $null
                    if ($msg.time) { try { $msgTime = [DateTimeOffset]::Parse($msg.time, [System.Globalization.CultureInfo]::InvariantCulture) } catch { } }
                    SqlExec -Conn $conn -Tx $tx -Query @"
INSERT INTO dbo.ChatMessage (ChatConversationId, MessageTime, SenderUsername, [Message], MessageUri)
VALUES (@cid, @t, @f, @msg, @u)
"@ -Params @{cid=$convId; t=$msgTime; f=$msg.from; msg=$msg.message; u=$msg.uri}
                    $msgCount++
                }
            }
            $tx.Commit()
            Write-Host "  Done: $chatCount conversations, $msgCount messages" -ForegroundColor Green
        } catch { $tx.Rollback(); throw }
    } else {
        Write-Host "  Skipped (already exists)" -ForegroundColor Gray
    }
}

# ============================================================
# 16. Wrapped2025 (store each section as JSON)
# ============================================================
Write-Host "Importing Wrapped2025..." -ForegroundColor Yellow
$wrapFile = Join-Path $DataPath "Wrapped2025.json"
if (Test-Path $wrapFile) {
    $existing = SqlScalar -Conn $conn -Tx $null -Query "SELECT COUNT(*) FROM dbo.Wrapped WHERE [Year] = 2025"
    if ($existing -eq 0) {
        $wrapData = Get-Content $wrapFile -Raw -Encoding UTF8 | ConvertFrom-Json
        $tx = $conn.BeginTransaction()
        $secCount = 0
        try {
            $wrapData | Get-Member -MemberType NoteProperty | ForEach-Object {
                $sectionName = $_.Name
                $sectionJson = ($wrapData.$sectionName | ConvertTo-Json -Depth 10 -Compress)
                SqlExec -Conn $conn -Tx $tx -Query @"
INSERT INTO dbo.Wrapped ([Year], SectionName, SectionData) VALUES (2025, @sn, @sd)
"@ -Params @{sn=$sectionName; sd=$sectionJson}
                $secCount++
            }
            $tx.Commit()
            Write-Host "  Done: $secCount sections" -ForegroundColor Green
        } catch { $tx.Rollback(); throw }
    } else {
        Write-Host "  Skipped (already exists)" -ForegroundColor Gray
    }
}

# ============================================================
# 17. YourSoundCapsule
# ============================================================
Write-Host "Importing YourSoundCapsule..." -ForegroundColor Yellow
$scFile = Join-Path $DataPath "YourSoundCapsule.json"
if (Test-Path $scFile) {
    $scData = Get-Content $scFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $existing = SqlScalar -Conn $conn -Tx $null -Query "SELECT COUNT(*) FROM dbo.SoundCapsuleStat"
    if ($existing -eq 0) {
        $tx = $conn.BeginTransaction()
        $statCount = 0; $hlCount = 0
        try {
            # Stats
            if ($scData.stats) {
                foreach ($stat in $scData.stats) {
                    $weekDate = if ($stat.date) { [DateTime]::Parse($stat.date) } else { $null }
                    $ttJson = if ($stat.topTracks -and $stat.topTracks.Count -gt 0) { ($stat.topTracks | ConvertTo-Json -Depth 5 -Compress) } else { "[]" }
                    $taJson = if ($stat.topArtists -and $stat.topArtists.Count -gt 0) { ($stat.topArtists | ConvertTo-Json -Depth 5 -Compress) } else { "[]" }
                    $tgJson = if ($stat.topGenres -and $stat.topGenres.Count -gt 0) { ($stat.topGenres | ConvertTo-Json -Depth 5 -Compress) } else { "[]" }
                    SqlExec -Conn $conn -Tx $tx -Query @"
INSERT INTO dbo.SoundCapsuleStat (WeekDate, StreamCount, SecondsPlayed, TopTracks, TopArtists, TopGenres)
VALUES (@d, @sc, @sp, @tt, @ta, @tg)
"@ -Params @{d=$weekDate; sc=$stat.streamCount; sp=$stat.secondsPlayed; tt=$ttJson; ta=$taJson; tg=$tgJson}
                    $statCount++
                }
            }
            # Highlights
            if ($scData.highlights) {
                foreach ($hl in $scData.highlights) {
                    $weekDate = if ($hl.date) { [DateTime]::Parse($hl.date) } else { $null }
                    $hlType = $hl.highlightType
                    # Extract the type-specific highlight data
                    $hlData = $null
                    $hl | Get-Member -MemberType NoteProperty | Where-Object { $_.Name -notin @('date','highlightType') } | ForEach-Object {
                        $hlData = ($hl.($_.Name) | ConvertTo-Json -Depth 5 -Compress)
                    }
                    if (-not $hlData) { $hlData = "{}" }
                    SqlExec -Conn $conn -Tx $tx -Query @"
INSERT INTO dbo.SoundCapsuleHighlight (WeekDate, HighlightType, HighlightData)
VALUES (@d, @ht, @hd)
"@ -Params @{d=$weekDate; ht=$hlType; hd=$hlData}
                    $hlCount++
                }
            }
            $tx.Commit()
            Write-Host "  Done: $statCount stats, $hlCount highlights" -ForegroundColor Green
        } catch { $tx.Rollback(); throw }
    } else {
        Write-Host "  Skipped (already exists)" -ForegroundColor Gray
    }
}

$stopwatch.Stop()
$conn.Close()
$conn.Dispose()

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Account Data Import Complete" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Time: $([math]::Round($stopwatch.Elapsed.TotalSeconds, 1))s"
