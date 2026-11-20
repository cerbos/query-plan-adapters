# Changelog

## Unreleased

- Fixed an over-grant on MySQL: comparing two string columns where one is mapped with an explicit
  null (`parent.aOptionalString == owner`) compared them under the columns' collation, so "set"
  matched "SET". They now compare byte for byte, as every other string comparison does.
