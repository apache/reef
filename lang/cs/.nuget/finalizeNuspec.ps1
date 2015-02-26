$nuspecdir = "nuspec"

# Delete the directory if it already exists
rmdir -Force -Recurse $nuspecdir

# Create directory for finalized nuspec files to live
mkdir -Force $nuspecdir

# copy over temporary nuspec files into new nuspec directory
$nuspecfiles = Get-Childitem -Recurse .. *.nuspec
$nuspecfiles | foreach { cp $_.FullName $nuspecDir }

# Replace package version token with actual version
