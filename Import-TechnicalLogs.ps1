<#
.SYNOPSIS
    Imports select Spotify Technical Log files into the Spotify SQL Server database.
    Focuses on the most analytically valuable log types.

.PARAMETER DataPath
    Path to the Spotify Technical Log Information folder.

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
    $DataPath = Join-Path $scriptDir "data\Spotify Technical Log Information"
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

function SqlExec {
    param($Conn, $Tx, [string]$Query, [hashtable]$Params = @{})
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
    $cmd.ExecuteNonQuery() | Out-Null
    $cmd.Dispose()
}

function Parse-Timestamp {
    param([string]$Value)
    if (-not $Value) { return $null }
    try {
        return [DateTimeOffset]::Parse($Value, [System.Globalization.CultureInfo]::InvariantCulture)
    } catch {
        try {
            # Try epoch milliseconds
            $epoch = [long]$Value
            return [DateTimeOffset]::FromUnixTimeMilliseconds($epoch)
        } catch {
            return $null
        }
    }
}

function Safe-String {
    param($Value, [int]$MaxLen = 200)
    if ($null -eq $Value) { return $null }
    $s = [string]$Value
    if ($s.Length -gt $MaxLen) { return $s.Substring(0, $MaxLen) }
    return $s
}

# Safely get a property from an object (returns $null if not present)
function Safe-Prop {
    param($Obj, [string]$Name)
    if ($null -eq $Obj) { return $null }
    $members = $Obj | Get-Member -MemberType NoteProperty -ErrorAction SilentlyContinue
    if ($members.Name -contains $Name) { return $Obj.$Name }
    return $null
}

# ============================================================
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Spotify Technical Log Import" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Server:    $ServerInstance"
Write-Host "Database:  $Database"
Write-Host "Data path: $DataPath"
Write-Host ""

$conn = Get-SqlConnection -Server $ServerInstance -Db $Database
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

# Helper to import a JSON array file
function Import-JsonArray {
    param(
        [string]$FileName,
        [string]$Label,
        [scriptblock]$InsertAction  # receives ($rec, $conn, $tx)
    )

    # Support multiple numbered files (e.g., Download.json, Download_1.json, Download_2.json)
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($FileName)
    $files = @()
    $primary = Join-Path $DataPath $FileName
    if (Test-Path $primary) { $files += Get-Item $primary }
    # Look for _1, _2, etc.
    $i = 1
    while ($true) {
        $numbered = Join-Path $DataPath "${baseName}_${i}.json"
        if (Test-Path $numbered) { $files += Get-Item $numbered; $i++ } else { break }
    }

    if ($files.Count -eq 0) {
        Write-Host "  $Label : File not found ($FileName)" -ForegroundColor Gray
        return 0
    }

    Write-Host "Importing $Label..." -ForegroundColor Yellow
    $totalCount = 0

    foreach ($file in $files) {
        $data = Get-Content $file.FullName -Raw -Encoding UTF8 | ConvertFrom-Json
        if (-not $data -or $data.Count -eq 0) {
            Write-Host "  $($file.Name): 0 records (empty)" -ForegroundColor Gray
            continue
        }

        $tx = $conn.BeginTransaction()
        $count = 0
        try {
            foreach ($rec in $data) {
                & $InsertAction $rec $conn $tx
                $count++
            }
            $tx.Commit()
            $totalCount += $count
            if ($files.Count -gt 1) {
                Write-Host "  $($file.Name): $count records" -ForegroundColor Gray
            }
        } catch {
            Write-Host "  ERROR in $($file.Name) at record $count : $_" -ForegroundColor Red
            try { $tx.Rollback() } catch { }
            throw
        }
    }

    Write-Host "  Done: $totalCount records" -ForegroundColor Green
    return $totalCount
}

# ============================================================
# 1. Collection Changes (AddedToCollection + RemovedFromCollection)
# ============================================================
$collectionTotal = 0

$collectionTotal += (Import-JsonArray -FileName "AddedToCollection.json" -Label "CollectionChange (added)" -InsertAction {
    param($rec, $c, $t)
    $ts = Parse-Timestamp (Safe-Prop $rec 'timestamp_utc')
    if (-not $ts) { $ts = Parse-Timestamp (Safe-Prop $rec 'context_time') }
    if (-not $ts) { return }
    SqlExec -Conn $c -Tx $t -Query @"
INSERT INTO dbo.CollectionChange (ChangeTime, ChangeType, CollectionSet, ItemUri, ContextUri)
VALUES (@ts, 'added', @s, @i, @ctx)
"@ -Params @{ts=$ts; s=(Safe-String (Safe-Prop $rec 'message_set') 20); i=(Safe-String (Safe-Prop $rec 'message_item_uri') 100); ctx=(Safe-String (Safe-Prop $rec 'message_context_uri') 100)}
})

$collectionTotal += (Import-JsonArray -FileName "RemovedFromCollection.json" -Label "CollectionChange (removed)" -InsertAction {
    param($rec, $c, $t)
    $ts = Parse-Timestamp (Safe-Prop $rec 'timestamp_utc')
    if (-not $ts) { $ts = Parse-Timestamp (Safe-Prop $rec 'context_time') }
    if (-not $ts) { return }
    SqlExec -Conn $c -Tx $t -Query @"
INSERT INTO dbo.CollectionChange (ChangeTime, ChangeType, CollectionSet, ItemUri, ContextUri)
VALUES (@ts, 'removed', @s, @i, @ctx)
"@ -Params @{ts=$ts; s=(Safe-String (Safe-Prop $rec 'message_set') 20); i=(Safe-String (Safe-Prop $rec 'message_item_uri') 100); ctx=(Safe-String (Safe-Prop $rec 'message_context_uri') 100)}
})

# ============================================================
# 2. Playlist Changes
# ============================================================
$playlistTotal = 0

$playlistTotal += (Import-JsonArray -FileName "AddedToPlaylist.json" -Label "PlaylistChange (added)" -InsertAction {
    param($rec, $c, $t)
    $ts = Parse-Timestamp (Safe-Prop $rec 'timestamp_utc')
    if (-not $ts) { $ts = Parse-Timestamp (Safe-Prop $rec 'context_time') }
    if (-not $ts) { return }
    SqlExec -Conn $c -Tx $t -Query @"
INSERT INTO dbo.PlaylistChange (ChangeTime, ChangeType, PlaylistUri, ItemUri, ItemUriKind, ClientPlatform)
VALUES (@ts, 'added', @pl, @i, @k, @cp)
"@ -Params @{ts=$ts; pl=(Safe-String (Safe-Prop $rec 'message_playlist_uri') 100); i=(Safe-String (Safe-Prop $rec 'message_item_uri') 100); k=(Safe-String (Safe-Prop $rec 'message_item_uri_kind') 20); cp=(Safe-String (Safe-Prop $rec 'message_client_platform') 50)}
})

$playlistTotal += (Import-JsonArray -FileName "RemovedFromPlaylist.json" -Label "PlaylistChange (removed)" -InsertAction {
    param($rec, $c, $t)
    $ts = Parse-Timestamp (Safe-Prop $rec 'timestamp_utc')
    if (-not $ts) { $ts = Parse-Timestamp (Safe-Prop $rec 'context_time') }
    if (-not $ts) { return }
    SqlExec -Conn $c -Tx $t -Query @"
INSERT INTO dbo.PlaylistChange (ChangeTime, ChangeType, PlaylistUri, ItemUri, ItemUriKind, ClientPlatform)
VALUES (@ts, 'removed', @pl, @i, @k, @cp)
"@ -Params @{ts=$ts; pl=(Safe-String (Safe-Prop $rec 'message_playlist_uri') 100); i=(Safe-String (Safe-Prop $rec 'message_item_uri') 100); k=(Safe-String (Safe-Prop $rec 'message_item_uri_kind') 20); cp=(Safe-String (Safe-Prop $rec 'message_client_platform') 50)}
})

# ============================================================
# 3. Rootlist Changes
# ============================================================
$rootlistTotal = 0

$rootlistTotal += (Import-JsonArray -FileName "AddedToRootlist.json" -Label "RootlistChange (added)" -InsertAction {
    param($rec, $c, $t)
    $ts = Parse-Timestamp (Safe-Prop $rec 'timestamp_utc')
    if (-not $ts) { $ts = Parse-Timestamp (Safe-Prop $rec 'context_time') }
    if (-not $ts) { return }
    SqlExec -Conn $c -Tx $t -Query @"
INSERT INTO dbo.RootlistChange (ChangeTime, ChangeType, ItemUri, ItemUriKind, ClientPlatform)
VALUES (@ts, 'added', @i, @k, @cp)
"@ -Params @{ts=$ts; i=(Safe-String (Safe-Prop $rec 'message_item_uri') 100); k=(Safe-String (Safe-Prop $rec 'message_item_uri_kind') 30); cp=(Safe-String (Safe-Prop $rec 'message_client_platform') 50)}
})

$rootlistTotal += (Import-JsonArray -FileName "RemovedFromRootlist.json" -Label "RootlistChange (removed)" -InsertAction {
    param($rec, $c, $t)
    $ts = Parse-Timestamp (Safe-Prop $rec 'timestamp_utc')
    if (-not $ts) { $ts = Parse-Timestamp (Safe-Prop $rec 'context_time') }
    if (-not $ts) { return }
    SqlExec -Conn $c -Tx $t -Query @"
INSERT INTO dbo.RootlistChange (ChangeTime, ChangeType, ItemUri, ItemUriKind, ClientPlatform)
VALUES (@ts, 'removed', @i, @k, @cp)
"@ -Params @{ts=$ts; i=(Safe-String (Safe-Prop $rec 'message_item_uri') 100); k=(Safe-String (Safe-Prop $rec 'message_item_uri_kind') 30); cp=(Safe-String (Safe-Prop $rec 'message_client_platform') 50)}
})

# ============================================================
# 4. Share Events
# ============================================================
Import-JsonArray -FileName "Share.json" -Label "ShareEvent" -InsertAction {
    param($rec, $c, $t)
    $ts = Parse-Timestamp (Safe-Prop $rec 'timestamp_utc')
    if (-not $ts) { $ts = Parse-Timestamp (Safe-Prop $rec 'context_time') }
    if (-not $ts) { return }
    SqlExec -Conn $c -Tx $t -Query @"
INSERT INTO dbo.ShareEvent (ShareTime, EntityUri, DestinationId, ShareId, SourcePage, SourcePageUri, DeviceType, OsName, OsVersion, Country)
VALUES (@ts, @eu, @di, @si, @sp, @spu, @dt, @os, @ov, @cc)
"@ -Params @{
        ts=$ts
        eu=(Safe-String (Safe-Prop $rec 'message_entity_uri') 100)
        di=(Safe-String (Safe-Prop $rec 'message_destination_id') 100)
        si=(Safe-String (Safe-Prop $rec 'message_share_id') 100)
        sp=(Safe-String (Safe-Prop $rec 'message_source_page') 100)
        spu=(Safe-String (Safe-Prop $rec 'message_source_page_uri') 200)
        dt=(Safe-String (Safe-Prop $rec 'context_device_type') 50)
        os=(Safe-String (Safe-Prop $rec 'context_os_name') 50)
        ov=(Safe-String (Safe-Prop $rec 'context_os_version') 50)
        cc=(Safe-String (Safe-Prop $rec 'context_conn_country') 2)
    }
} | Out-Null

# ============================================================
# 5. Playback Errors
# ============================================================
Import-JsonArray -FileName "PlaybackError.json" -Label "PlaybackError" -InsertAction {
    param($rec, $c, $t)
    $ts = Parse-Timestamp (Safe-Prop $rec 'timestamp_utc')
    if (-not $ts) { $ts = Parse-Timestamp (Safe-Prop $rec 'context_time') }
    if (-not $ts) { return }
    $bitrate = $null
    $brVal = Safe-Prop $rec 'message_bitrate'
    if ($brVal) { try { $bitrate = [int]$brVal } catch { } }
    $fatal = $null
    $fVal = Safe-Prop $rec 'message_fatal'
    if ($null -ne $fVal) { $fatal = [bool]$fVal }
    SqlExec -Conn $c -Tx $t -Query @"
INSERT INTO dbo.PlaybackError (ErrorTime, FileId, TrackId, ErrorCode, IsFatal, Bitrate, DeviceType, OsName)
VALUES (@ts, @fi, @ti, @ec, @fa, @br, @dt, @os)
"@ -Params @{
        ts=$ts
        fi=(Safe-String (Safe-Prop $rec 'message_file_id') 100)
        ti=(Safe-String (Safe-Prop $rec 'message_track_id') 100)
        ec=(Safe-String (Safe-Prop $rec 'message_error_code') 50)
        fa=$fatal
        br=$bitrate
        dt=(Safe-String (Safe-Prop $rec 'context_device_type') 50)
        os=(Safe-String (Safe-Prop $rec 'context_os_name') 50)
    }
} | Out-Null

# ============================================================
# 6. Sessions
# ============================================================
Import-JsonArray -FileName "SessionCreation.json" -Label "Session" -InsertAction {
    param($rec, $c, $t)
    $ts = Parse-Timestamp (Safe-Prop $rec 'timestamp_utc')
    if (-not $ts) { $ts = Parse-Timestamp (Safe-Prop $rec 'context_time') }
    if (-not $ts) { return }
    SqlExec -Conn $c -Tx $t -Query @"
INSERT INTO dbo.Session (SessionTime, SpotifySessionId, CreatedAt)
VALUES (@ts, @sid, @ca)
"@ -Params @{ts=$ts; sid=(Safe-String (Safe-Prop $rec 'message_session_id') 100); ca=(Safe-String (Safe-Prop $rec 'message_created_at') 50)}
} | Out-Null

# ============================================================
# 7. Account Pages Activity
# ============================================================
Import-JsonArray -FileName "AccountPagesActivity.json" -Label "AccountActivity" -InsertAction {
    param($rec, $c, $t)
    $ts = Parse-Timestamp (Safe-Prop $rec 'timestamp_utc')
    if (-not $ts) { $ts = Parse-Timestamp (Safe-Prop $rec 'context_time') }
    if (-not $ts) { return }
    $success = $null
    $sVal = Safe-Prop $rec 'message_success'
    if ($null -ne $sVal) { $success = [bool]$sVal }
    SqlExec -Conn $c -Tx $t -Query @"
INSERT INTO dbo.AccountActivity (ActivityTime, ActivityName, Market, Success, Reason, DeviceType, OsName, Country)
VALUES (@ts, @an, @mk, @su, @re, @dt, @os, @cc)
"@ -Params @{
        ts=$ts
        an=(Safe-String (Safe-Prop $rec 'message_name') 200)
        mk=(Safe-String (Safe-Prop $rec 'message_market') 10)
        su=$success
        re=(Safe-String (Safe-Prop $rec 'message_reason') 200)
        dt=(Safe-String (Safe-Prop $rec 'context_device_type') 50)
        os=(Safe-String (Safe-Prop $rec 'context_os_name') 50)
        cc=(Safe-String (Safe-Prop $rec 'context_conn_country') 2)
    }
} | Out-Null

$stopwatch.Stop()
$conn.Close()
$conn.Dispose()

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Technical Log Import Complete" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Time: $([math]::Round($stopwatch.Elapsed.TotalSeconds, 1))s"
