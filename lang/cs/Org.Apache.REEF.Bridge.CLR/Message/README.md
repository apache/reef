# Java/C# Interop Avro Messages

### DO NOT EDIT THESE C# FILES

The C# files in this directory are generated during the build from the Avro
record definition files in lang/common/bridge/avro. When adding a new .avsc
file in lang/common/bridge/avro you must also put an empty .cs file here
with the same name as the .cs file is the dependency. For example,

  lang/common/bridge/avro/MyMsg.avsc -> lang/common/bridge/avro/MyMsg.cs
