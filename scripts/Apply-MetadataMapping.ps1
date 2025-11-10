<#
.SYNOPSIS
    Applies Author/Editor metadata to SharePoint files using PnP.PowerShell based on a CSV mapping.

.DESCRIPTION
    Expects a CSV (e.g. mapping.csv generated from Dropbox export) containing:
        FilePath   - Server-relative SharePoint path (e.g. /sites/YourSite/Shared Documents/folder/file.docx)
        AuthorUPN  - User principal name for the desired Created By value
        EditorUPN  - User principal name for the desired Modified By value

    For each row, the script resolves the users (falling back to a specified admin UPN when missing)
    and updates the list item's Author/Editor fields. A -WhatIf switch supports dry-run mode.

.PARAMETER SiteUrl
    Full URL to the SharePoint site hosting the target library.

.PARAMETER MappingCsv
    Path to the CSV file containing FilePath, AuthorUPN, and EditorUPN columns.

.PARAMETER FallbackUpn
    Default UPN to use when a mapping row does not provide a user. Defaults to the SharePoint admin.

.PARAMETER WhatIf
    When supplied, performs a dry run without committing changes.

.EXAMPLE
    .\Apply-MetadataMapping.ps1 -SiteUrl "https://contoso.sharepoint.com/sites/Docs" `
        -MappingCsv "C:\Temp\mapping.csv" -FallbackUpn "sharepointadmin@contoso.com"

.NOTES
    Requires PnP.PowerShell module and sufficient permissions to update list items.
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$SiteUrl,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$MappingCsv,

    [Parameter()]
    [ValidateNotNullOrEmpty()]
    [string]$FallbackUpn = "sharepoint.admin@ducorpgroup.com",

    [Parameter()]
    [string]$ClientId,

    [Parameter()]
    [string]$Tenant,

    [Parameter()]
    [object]$ExistingConnection
)

if (-not (Get-Module -Name PnP.PowerShell, SharePointPnPPowerShellOnline -ListAvailable)) {
    throw "PnP PowerShell module is not installed. Install with 'Install-Module PnP.PowerShell' (PowerShell 7) or 'Install-Module SharePointPnPPowerShellOnline' (Windows PowerShell)."
}

$script:UsingModule = $null
$script:UserCache = @{}
$script:UserCacheInitialized = $false

if (-not (Get-Module -Name PnP.PowerShell)) {
    try {
        Import-Module PnP.PowerShell -ErrorAction Stop | Out-Null
        $script:UsingModule = "PnP.PowerShell"
    } catch {
        Import-Module SharePointPnPPowerShellOnline -ErrorAction Stop | Out-Null
        $script:UsingModule = "SharePointPnPPowerShellOnline"
    }
} else {
    $script:UsingModule = "PnP.PowerShell"
}

if (-not $script:UsingModule) {
    throw "Unable to load a PnP PowerShell module."
}

function Resolve-Upn {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Candidate,
        [Parameter(Mandatory = $true)]
        [string]$Fallback
    )

    if ($null -eq $Candidate) {
        return $Fallback
    }

    $candidateTrimmed = $Candidate.Trim()
    if ([string]::IsNullOrWhiteSpace($candidateTrimmed)) {
        return $Fallback
    }

    return $candidateTrimmed
}

function Add-UserToCache {
    param(
        [Parameter(Mandatory = $true)]
        [string]$LoginName,
        [Parameter()]
        [string]$Email
    )

    if ([string]::IsNullOrWhiteSpace($LoginName)) {
        return
    }

    $loginKey = $LoginName.ToLowerInvariant()
    $script:UserCache[$loginKey] = $LoginName

    if ($LoginName -like "*|*") {
        $segments = $LoginName.Split("|")
        $lastSegment = $segments[-1]
        if (-not [string]::IsNullOrWhiteSpace($lastSegment)) {
            $script:UserCache[$lastSegment.ToLowerInvariant()] = $LoginName
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($Email)) {
        $script:UserCache[$Email.ToLowerInvariant()] = $LoginName
    }
}

function Initialize-UserCache {
    if ($script:UserCacheInitialized) {
        return
    }
    try {
        $users = @(Get-PnPUser)
        foreach ($u in $users) {
            Add-UserToCache -LoginName $u.LoginName -Email $u.Email
        }
        $script:UserCacheInitialized = $true
    } catch {
        Write-Warning "Unable to preload site users: $($_.Exception.Message)"
    }
}

function Resolve-UserLogin {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Upn
    )

    if ([string]::IsNullOrWhiteSpace($Upn)) {
        return $null
    }

    $key = $Upn.ToLowerInvariant()
    if ($script:UserCache.ContainsKey($key)) {
        return $script:UserCache[$key]
    }

    Initialize-UserCache
    if ($script:UserCache.ContainsKey($key)) {
        return $script:UserCache[$key]
    }

    $resolvedLogin = $null

    try {
        $userByIdentity = Get-PnPUser -Identity $Upn -ErrorAction Stop
        if ($userByIdentity -and $userByIdentity.LoginName) {
            $resolvedLogin = $userByIdentity.LoginName
        }
    } catch {
        # ignore, we will try other methods
    }

    if (-not $resolvedLogin -and (Get-Command -Name Ensure-PnPUser -ErrorAction SilentlyContinue)) {
        try {
            $ensured = Ensure-PnPUser -LoginName $Upn -ErrorAction Stop
            if ($ensured -and $ensured.LoginName) {
                $resolvedLogin = $ensured.LoginName
            }
        } catch {
            Write-Warning "Ensure-PnPUser failed for $Upn : $($_.Exception.Message)"
        }
    }

    if (-not $resolvedLogin -and (Get-Command -Name Resolve-PnPUser -ErrorAction SilentlyContinue)) {
        try {
            $resolved = Resolve-PnPUser -Identity $Upn -ErrorAction Stop
            if ($resolved -and $resolved.LoginName) {
                $resolvedLogin = $resolved.LoginName
            }
        } catch {
            Write-Warning "Resolve-PnPUser failed for $Upn : $($_.Exception.Message)"
        }
    }

    if (-not $resolvedLogin) {
        $membershipKey = "i:0#.f|membership|$key"
        if ($script:UserCache.ContainsKey($membershipKey)) {
            $resolvedLogin = $script:UserCache[$membershipKey]
        }
    }

    if ($resolvedLogin) {
        Add-UserToCache -LoginName $resolvedLogin -Email $Upn
        return $resolvedLogin
    }

    Write-Warning "Unable to resolve user '$Upn' via available PnP commands."
    return $null
}

function Get-ListPipeBind {
    param(
        [Parameter(Mandatory = $true)]
        $ListItem,
        [Parameter(Mandatory = $true)]
        [string]$FilePath
    )

    if ($ListItem -and $ListItem.PSObject.Properties["ParentListId"] -and $ListItem.ParentListId) {
        return $ListItem.ParentListId
    }

    if ($ListItem -and $ListItem.PSObject.Properties["ParentReference"] -and $ListItem.ParentReference.ListId) {
        return $ListItem.ParentReference.ListId
    }

    $relativePath = $FilePath.TrimStart("/")
    $segments = $relativePath -split "/"

    if ($segments.Length -ge 1 -and $segments[0] -eq "sites") {
        if ($segments.Length -ge 3) {
            return [System.Uri]::UnescapeDataString($segments[2])
        }
    }

    if ($segments.Length -ge 1) {
        return [System.Uri]::UnescapeDataString($segments[0])
    }

    return $null
}

function Convert-ToPnPDate {
    param(
        [Parameter()]
        [string]$Value
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $null
    }

    $formats = @(
        "yyyy-MM-ddTHH:mm:ssZ",
        "yyyy-MM-ddTHH:mm:ss",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd HH:mm",
        "yyyy/MM/dd HH:mm:ss",
        "yyyy/MM/dd HH:mm",
        "dd-MM-yyyy HH:mm:ss",
        "dd-MM-yyyy HH:mm",
        "dd/MM/yyyy HH:mm:ss",
        "dd/MM/yyyy HH:mm"
    )

    foreach ($format in $formats) {
        try {
            return [datetime]::ParseExact(
                $Value,
                $format,
                [System.Globalization.CultureInfo]::InvariantCulture,
                [System.Globalization.DateTimeStyles]::AssumeLocal
            )
        } catch {
            continue
        }
    }

    try {
        return [datetime]::Parse($Value, [System.Globalization.CultureInfo]::InvariantCulture)
    } catch {
        try {
            return Get-Date $Value
        } catch {
            Write-Warning "Unable to parse datetime value '$Value'."
            return $null
        }
    }
}

if (-not (Test-Path -Path $MappingCsv)) {
    throw "Mapping CSV not found at '$MappingCsv'."
}

$dryRun = $PSBoundParameters.ContainsKey("WhatIf")

if ($ExistingConnection) {
    if (Get-Command -Name Set-PnPConnection -ErrorAction SilentlyContinue) {
        Set-PnPConnection -Connection $ExistingConnection
    } else {
        throw "An existing connection was provided but Set-PnPConnection is unavailable."
    }
} else {
    Write-Host "Connecting to $SiteUrl ..." -ForegroundColor Cyan
    try {
        if ($script:UsingModule -eq "PnP.PowerShell") {
            if ($ClientId -and $Tenant) {
                Connect-PnPOnline -Url $SiteUrl -ClientId $ClientId -Tenant $Tenant -Interactive
            } else {
                Connect-PnPOnline -Url $SiteUrl -Interactive
            }
        } else {
            if ($ClientId -and $Tenant) {
                Connect-PnPOnline -Url $SiteUrl -ClientId $ClientId -Tenant $Tenant -UseWebLogin:$false -Interactive
            } else {
                Connect-PnPOnline -Url $SiteUrl -UseWebLogin
            }
        }
    } catch {
        throw "Failed to connect to SharePoint: $($_.Exception.Message)"
    }
}

try {
    $rows = Import-Csv -Path $MappingCsv
    if (-not $rows) {
        Write-Warning "No rows found in mapping CSV."
        return
    }

    $rowIndex = 0
    foreach ($row in $rows) {
        $rowIndex++
        if ($null -eq $row.FilePath) {
            $filePath = ""
        } else {
            $filePath = $row.FilePath
        }
        $filePath = $filePath.Trim()
        if ([string]::IsNullOrWhiteSpace($filePath)) {
            Write-Warning "Row $rowIndex skipped: FilePath is blank."
            continue
        }

        $authorCandidate = $row.AuthorUPN
        if ([string]::IsNullOrWhiteSpace($authorCandidate) -and $row.created_by_email) {
            $authorCandidate = $row.created_by_email
        }
        if ([string]::IsNullOrWhiteSpace($authorCandidate) -and $row.created_by_id) {
            $authorCandidate = $row.created_by_id
        }
        $authorUpn = Resolve-Upn -Candidate $authorCandidate -Fallback $FallbackUpn

        $editorCandidate = $row.EditorUPN
        if ([string]::IsNullOrWhiteSpace($editorCandidate) -and $row.last_modified_by_email) {
            $editorCandidate = $row.last_modified_by_email
        }
        if ([string]::IsNullOrWhiteSpace($editorCandidate) -and $row.last_modified_by_id) {
            $editorCandidate = $row.last_modified_by_id
        }
        $editorUpn = Resolve-Upn -Candidate $editorCandidate -Fallback $authorUpn

        $createdDate = Convert-ToPnPDate -Value $row.created_client_modified
        if (-not $createdDate) {
            $createdDate = Convert-ToPnPDate -Value $row.created_server_modified
        }

        $modifiedDate = $null
        if ($row.last_client_modified -or $row.last_server_modified) {
            $modifiedDate = Convert-ToPnPDate -Value $row.last_client_modified
            if (-not $modifiedDate) {
                $modifiedDate = Convert-ToPnPDate -Value $row.last_server_modified
            }
        }

        try {
            $listItem = Get-PnPFile -Url $filePath -AsListItem -ErrorAction Stop

            $authorLogin = Resolve-UserLogin -Upn $authorUpn
            if (-not $authorLogin) {
                Write-Warning "Unable to resolve author UPN '$authorUpn'; skipping row $rowIndex."
                continue
            }

            $editorLogin = Resolve-UserLogin -Upn $editorUpn
            if (-not $editorLogin) {
                Write-Warning "Unable to resolve editor UPN '$editorUpn'; skipping row $rowIndex."
                continue
            }

            $message = "Item ID $($listItem.Id) [$filePath] -> Author: $authorLogin; Editor: $editorLogin"
            if ($createdDate) {
                $message += "; Created: $createdDate"
            }
            if ($modifiedDate) {
                $message += "; Modified (skipped): $modifiedDate"
            }

            if ($PSCmdlet.ShouldProcess($filePath, "Update Author/Editor metadata")) {
                if ($dryRun) {
                    Write-Host "[Dry-Run] $message" -ForegroundColor Yellow
                } else {
                    $listPipeBind = Get-ListPipeBind -ListItem $listItem -FilePath $filePath
                    if (-not $listPipeBind) {
                        Write-Warning "Unable to determine list for file '$filePath'; skipping row $rowIndex."
                        continue
                    }

                            $ctx = Get-PnPContext
                            $listItemRef = Get-PnPListItem -List $listPipeBind -Id $listItem.Id

                            $authorFieldValue = $null
                            $editorFieldValue = $null

                            try {
                                $authorUser = $ctx.Web.EnsureUser($authorLogin)
                                $editorUser = $ctx.Web.EnsureUser($editorLogin)
                                $ctx.Load($authorUser)
                                $ctx.Load($editorUser)
                                $ctx.ExecuteQuery()

                                $authorFieldValue = New-Object Microsoft.SharePoint.Client.FieldUserValue
                                $authorFieldValue.LookupId = $authorUser.Id
                                $editorFieldValue = New-Object Microsoft.SharePoint.Client.FieldUserValue
                            $editorFieldValue.LookupId = $editorUser.Id
                            } catch {
                                Write-Warning "Failed to resolve users for CSOM update: $($_.Exception.Message)"
                                continue
                            }

                            if ($createdDate) {
                                $listItemRef["Created"] = $createdDate
                            }

                            $ctx.Load($listItemRef)
                            $ctx.ExecuteQuery()

                            $authorInternal = $listItemRef.FieldValues["Author"]
                            $editorInternal = $listItemRef.FieldValues["Editor"]

                            if ($authorInternal) {
                                $authorInternal.LookupId = $authorFieldValue.LookupId
                            }
                            if ($editorInternal) {
                                $editorInternal.LookupId = $editorFieldValue.LookupId
                            }

                            $listItemRef["Author"] = $authorInternal
                            $listItemRef["Editor"] = $editorInternal

                            if ($createdDate) {
                                $listItemRef["Created"] = $createdDate
                            }

                            $listItemRef.SystemUpdate()
                            $ctx.ExecuteQuery()
                            $ctx.ExecuteQuery()

                    Write-Host "[Updated] $message" -ForegroundColor Green
                }
            }
        } catch {
            Write-Warning "Failed for $filePath : $($_.Exception.Message)"
        }
    }
}
finally {
    Disconnect-PnPOnline
    Write-Host "Disconnected from $SiteUrl." -ForegroundColor Cyan
}

