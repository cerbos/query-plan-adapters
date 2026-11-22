# Changelog

## Unreleased

- Fixed an over-grant on SQLite: a timestamp literal finer than a millisecond
  (`timestamp(createdAt) >= timestamp("2020-03-15T10:30:00.000001Z")`) was bound at the millisecond
  Exposed stores there, so a row at `10:30:00.000` matched. The comparison is now solved at millisecond
  precision on SQLite, as it is at microsecond precision elsewhere.
- Fixed an over-grant on SQLite: `int()` over a signed string holding a NUL (`"-7\u0000x"`) read only the
  part before the NUL and converted it. Such a string is now an error, as in CEL.
- Fixed an over-grant on SQLite: `matches()` over a string holding a NUL (`"One\u0000x".matches("^o|e$")`)
  read the string only up to the NUL, so `e$` matched. A pattern of one part now compares every byte, and a
  pattern of several parts (`^.*a$`) denies a string holding a NUL, which it cannot compare exactly.
- Fixed an over-grant on MySQL: comparing two string columns where one is mapped with an explicit
  null (`parent.aOptionalString == owner`) compared them under the columns' collation, so "set"
  matched "SET". They now compare byte for byte, as every other string comparison does.
