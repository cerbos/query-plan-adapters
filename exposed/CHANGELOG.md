# Changelog

## Unreleased

- Fixed an over-grant on SQLite: `matches()` over a string holding a NUL (`"One\u0000x".matches("^o|e$")`)
  read the string only up to the NUL, so `e$` matched. A pattern of one part now compares every byte, and a
  pattern of several parts (`^.*a$`) denies a string holding a NUL, which it cannot compare exactly.
- Fixed an over-grant on MySQL: comparing two string columns where one is mapped with an explicit
  null (`parent.aOptionalString == owner`) compared them under the columns' collation, so "set"
  matched "SET". They now compare byte for byte, as every other string comparison does.
