<#
.SYNOPSIS
    Imports ALL Spotify Technical Log JSON files into the Spotify SQL Server database.
    High-value log types get dedicated tables. All remaining logs go into a generic
    TechLogEvent table with common context columns and a JSON MessageData column.

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

function SqlScalar {
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
    $result = $cmd.ExecuteScalar()
    $cmd.Dispose()
    return $result
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

# ============================================================
# 8. Recipients (special structure - not an array)
# ============================================================
Write-Host "Importing Recipients..." -ForegroundColor Yellow
$recipFile = Join-Path $DataPath "recipients.json"
if (Test-Path $recipFile) {
    $recipData = Get-Content $recipFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $existing = SqlScalar -Conn $conn -Tx $null -Query "SELECT COUNT(*) FROM dbo.DataRecipient"
    if ($existing -eq 0 -and $recipData.recipients) {
        $tx = $conn.BeginTransaction()
        $recipCount = 0
        try {
            foreach ($group in $recipData.recipients) {
                $groupName = Safe-Prop $group 'name'
                $members = Safe-Prop $group 'members'
                if ($members) {
                    foreach ($member in $members) {
                        SqlExec -Conn $conn -Tx $tx -Query @"
INSERT INTO dbo.DataRecipient (GroupName, MemberName) VALUES (@g, @m)
"@ -Params @{g=(Safe-String $groupName 200); m=(Safe-String $member 300)}
                        $recipCount++
                    }
                }
            }
            $tx.Commit()
            Write-Host "  Done: $recipCount records" -ForegroundColor Green
        } catch { $tx.Rollback(); throw }
    } else {
        Write-Host "  Skipped (already exists or empty)" -ForegroundColor Gray
    }
}

# ============================================================
# 9. ALL Remaining JSON files -> TechLogEvent (generic table)
# ============================================================
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Importing remaining logs -> TechLogEvent" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Files already imported into dedicated tables (skip these)
$dedicatedFiles = @(
    'AddedToCollection', 'RemovedFromCollection',
    'AddedToPlaylist', 'RemovedFromPlaylist',
    'AddedToRootlist', 'RemovedFromRootlist',
    'Share', 'PlaybackError', 'SessionCreation', 'AccountPagesActivity',
    'recipients'
)

# Context property names to extract into typed columns
$contextProps = @(
    'context_application_version', 'context_conn_country',
    'context_device_manufacturer', 'context_device_model',
    'context_device_type', 'context_os_name', 'context_os_version',
    'context_user_agent', 'context_time', 'context_receiver_service_timestamp'
)

# Gather all JSON files, group by base name
$allJsonFiles = Get-ChildItem -Path $DataPath -Filter "*.json" | Sort-Object Name
$fileGroups = @{}
foreach ($f in $allJsonFiles) {
    $baseName = $f.BaseName -replace '_\d+$', ''
    if ($dedicatedFiles -contains $baseName) { continue }
    if (-not $fileGroups.ContainsKey($baseName)) { $fileGroups[$baseName] = @() }
    $fileGroups[$baseName] += $f
}

Write-Host "Found $($fileGroups.Count) remaining log types to import" -ForegroundColor Yellow

# Use bulk insert with SqlBulkCopy for performance
$grandTotal = 0

# Prepare the INSERT query
$insertQuery = @"
INSERT INTO dbo.TechLogEvent (LogType, TimestampUtc, ContextTime, AppVersion, ConnCountry,
    DeviceManufacturer, DeviceModel, DeviceType, OsName, OsVersion, UserAgent, MessageData, SourceFile)
VALUES (@lt, @ts, @ct, @av, @cc, @dm, @dmod, @dt, @os, @ov, @ua, @md, @sf)
"@

foreach ($logType in ($fileGroups.Keys | Sort-Object)) {
    $files = $fileGroups[$logType]
    $typeTotal = 0

    foreach ($file in $files) {
        $rawJson = Get-Content $file.FullName -Raw -Encoding UTF8
        if (-not $rawJson -or $rawJson.Trim() -eq "" -or $rawJson.Trim() -eq "[]") {
            continue
        }

        $data = $null
        try {
            $data = $rawJson | ConvertFrom-Json
        } catch {
            Write-Host "  WARN: Could not parse $($file.Name): $_" -ForegroundColor DarkYellow
            continue
        }

        # Handle both arrays and single objects
        if ($data -isnot [array]) { $data = @($data) }
        if ($data.Count -eq 0) { continue }

        $tx = $conn.BeginTransaction()
        $count = 0
        try {
            foreach ($rec in $data) {
                # Extract timestamp
                $tsUtc = $null
                $tsVal = Safe-Prop $rec 'timestamp_utc'
                if ($tsVal) {
                    try { $tsUtc = [DateTimeOffset]::Parse($tsVal, [System.Globalization.CultureInfo]::InvariantCulture) } catch { }
                }

                # Extract context_time as epoch
                $ctEpoch = $null
                $ctVal = Safe-Prop $rec 'context_time'
                if ($null -ne $ctVal) {
                    try { $ctEpoch = [long]$ctVal } catch { }
                }

                # Build message data JSON from all non-context, non-timestamp properties
                $msgProps = @{}
                $members = $rec | Get-Member -MemberType NoteProperty -ErrorAction SilentlyContinue
                foreach ($member in $members) {
                    $propName = $member.Name
                    if ($propName -eq 'timestamp_utc') { continue }
                    if ($propName -like 'context_*') { continue }
                    $propVal = $rec.$propName
                    if ($null -ne $propVal) {
                        $msgProps[$propName] = $propVal
                    }
                }
                $msgJson = if ($msgProps.Count -gt 0) { ($msgProps | ConvertTo-Json -Depth 5 -Compress) } else { $null }

                SqlExec -Conn $conn -Tx $tx -Query $insertQuery -Params @{
                    lt  = $logType
                    ts  = $tsUtc
                    ct  = $ctEpoch
                    av  = (Safe-String (Safe-Prop $rec 'context_application_version') 50)
                    cc  = (Safe-String (Safe-Prop $rec 'context_conn_country') 2)
                    dm  = (Safe-String (Safe-Prop $rec 'context_device_manufacturer') 100)
                    dmod = (Safe-String (Safe-Prop $rec 'context_device_model') 100)
                    dt  = (Safe-String (Safe-Prop $rec 'context_device_type') 50)
                    os  = (Safe-String (Safe-Prop $rec 'context_os_name') 50)
                    ov  = (Safe-String (Safe-Prop $rec 'context_os_version') 50)
                    ua  = (Safe-String (Safe-Prop $rec 'context_user_agent') 500)
                    md  = $msgJson
                    sf  = $file.Name
                }
                $count++
            }
            $tx.Commit()
            $typeTotal += $count
        } catch {
            Write-Host "  ERROR in $($file.Name) at record $count : $_" -ForegroundColor Red
            try { $tx.Rollback() } catch { }
            throw
        }
    }

    if ($typeTotal -gt 0) {
        Write-Host "  $logType : $typeTotal records" -ForegroundColor Gray
    }
    $grandTotal += $typeTotal
}

Write-Host ""
Write-Host "  TechLogEvent total: $grandTotal records" -ForegroundColor Green

$stopwatch.Stop()
$conn.Close()
$conn.Dispose()

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Technical Log Import Complete" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Time: $([math]::Round($stopwatch.Elapsed.TotalSeconds, 1))s"
Write-Host "  Dedicated tables: CollectionChange, PlaylistChange, RootlistChange,"
Write-Host "                    ShareEvent, PlaybackError, Session, AccountActivity"
Write-Host "  Recipients:       (see above)"
Write-Host "  TechLogEvent:     $grandTotal records across $($fileGroups.Count) log types"
