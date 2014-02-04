Param(
  # the main class to run
  [string]$Class,
  # the jar file with the project inside
  [string]$Jar
)

# Sanitize the classpath returned by "yarn classpath"
$yarn_classpath = (yarn classpath).split(";") | where {$_ -ne ""} | Unique
$yarn_classpath += $Jar

# Assemble the classpath for the job
$java_classpath = $yarn_classpath -join ';'

# Assemble the command to run
# Note: We need to put the classpath within "", as it contains ";"
$command = "$env:JAVA_HOME\bin\java.exe -cp `"$java_classpath`"  $Class"
Invoke-Expression -Command $command
