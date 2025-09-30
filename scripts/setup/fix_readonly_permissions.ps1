# PowerShell script to find and fix read-only directories and files
# Run this script from the project root directory

Write-Host "🔍 Scanning for read-only directories and files..." -ForegroundColor Green

# Function to remove read-only attribute from a path
function Remove-ReadOnlyAttribute {
    param (
        [string]$Path
    )

    if (Test-Path $Path) {
        # Fix read-only attribute on the root path itself (file or directory)
        $item = Get-Item -LiteralPath $Path
        if ($item.Attributes -band [System.IO.FileAttributes]::ReadOnly) {
            $item.Attributes = $item.Attributes -band (-bnot [System.IO.FileAttributes]::ReadOnly)
            Write-Host "✅ Fixed: $($item.FullName)" -ForegroundColor Yellow
        }

        # Fix read-only attribute on all child files and directories
        Get-ChildItem -Path $Path -Recurse -Force | ForEach-Object {
            if ($_.Attributes -band [System.IO.FileAttributes]::ReadOnly) {
                $_.Attributes = $_.Attributes -band (-bnot [System.IO.FileAttributes]::ReadOnly)
                Write-Host "✅ Fixed: $($_.FullName)" -ForegroundColor Yellow
            }
        }
    } else {
        Write-Host "❌ Path not found: $Path" -ForegroundColor Red
    }
}

# Paths to scan
$projectPath = "."

# Fix read-only attributes for the entire project
Write-Host "📂 Fixing read-only attributes for the project..." -ForegroundColor Cyan
Remove-ReadOnlyAttribute -Path $projectPath

Write-Host "🎉 All read-only attributes have been fixed!" -ForegroundColor Green
Write-Host "You can now edit files and directories without issues." -ForegroundColor Cyan
