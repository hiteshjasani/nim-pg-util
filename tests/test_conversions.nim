import unittest

import times, pg_util

suite "Timestamp Conversion":
  test "to PG Timestamp":
    check("2020-02-22 18:42:19.000Z" ==
          toPgTimestamp(initDateTime(22, mFeb, 2020, 18, 42, 19, 0, utc())))
  test "from PG Timestamp":
    check(initDateTime(22, mFeb, 2020, 18, 42, 19, 0, utc()) ==
          fromPgTimestamp("2020-02-22 18:42:19Z"))
    check(initDateTime(22, mFeb, 2020, 18, 42, 19, 0, utc()) ==
          fromPgTimestamp("2020-02-22 18:42:19.000Z"))

suite "Bool Conversion":
  test "From PG bool == true":
    check fromPgBool("t")
    check fromPgBool("T")
    check fromPgBool("tRue")
    check fromPgBool("Y")
    check fromPgBool("y")
    check fromPgBool("yes")
    check fromPgBool("1")
    check fromPgBool("oN")

  test "From PG bool == false":
    check fromPgBool("") == false
    check fromPgBool("f") == false
    check fromPgBool("fAlse") == false
    check fromPgBool("0") == false
    check fromPgBool("n") == false
    check fromPgBool("nO") == false
