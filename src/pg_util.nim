import db_postgres, times
from strutils import toLowerAscii
from postgres import pqfinish, pqreset, pqstatus, CONNECTION_OK

proc openDb*(uri: string): DbConn =
  ## Open database using a single URI with parameters
  ##
  ##   postgresql://[user[:password]@][netloc][:port][,...][/dbname][?param1=value1&...]
  ##
  ## Example URI's
  ##
  ##   postgresql://
  ##   postgresql://localhost
  ##   postgresql://localhost:5433
  ##   postgresql://localhost/mydb
  ##   postgresql://user@localhost
  ##   postgresql://user:secret@localhost
  ##   postgresql://other@localhost/otherdb?connect_timeout=10&application_name=myapp
  ##   postgresql://host1:123,host2:456/somedb?target_session_attrs=any&application_name=myapp
  ##
  ## See Section 33.1.1 Connection Strings
  ##   https://www.postgresql.org/docs/current/libpq-connect.html
  ##
  open("", "", "", uri)

proc resetDb*(db: DbConn) =
  ## Resets the communications channel to the server
  pqreset(db)

proc closeDb*(db: DbConn) =
  ## Close database connection
  pqfinish(db)

proc isConnectionOK*(db: DbConn): bool =
  ## Do we still have a connection to the server?
  pqstatus(db) == CONNECTION_OK

proc needsReconnect*(db: DbConn): bool =
  ## Have we lost connectivity to the server?
  not isConnectionOK(db)

proc startTx*(db: DbConn): bool =
  ## Start transaction
  tryExec(db, sql"start transaction")

proc isoSerializable*(db: DbConn): bool =
  ## Set transaction isolation to serializable
  tryExec(db, sql"set transaction isolation level serializable")

proc commitTx*(db: DbConn): bool =
  ## Commit transaction
  tryExec(db, sql"commit")

proc abortTx*(db: DbConn): bool =
  ## Abort transaction
  tryExec(db, sql"abort")

proc fromPgTimestamp*(pgTimestamp: string): DateTime =
  ## Parse postgres timestamp with time zone into a DateTime.
  ##
  ## Will try to parse columns defined as:
  ##   timestamp (0) with time zone
  ##   timestamp (3) with time zone
  ##
  ## See https://www.postgresql.org/docs/current/datatype-datetime.html
  ##
  try:
    parse(pgTimestamp, "yyyy-MM-dd' 'HH:mm:sszz")
  except Exception:
    parse(pgTimestamp, "yyyy-MM-dd' 'HH:mm:ss'.'fffzz")

proc toPgTimestamp*(dt: DateTime): string =
  ## Convert DateTime into a Postgres timestamp.
  ##
  ## This will work with columns defined as:
  ##   timestamp (0) with time zone
  ##   timestamp (3) with time zone
  ##
  ## See https://www.postgresql.org/docs/current/datatype-datetime.html
  ##
  format(dt, "yyyy-MM-dd' 'HH:mm:ss'.'fffzz")

proc fromPgBool*(pgStr: string): bool =
  ## Parse a boolean column value.  The states map as follows:
  ##
  ##   True - true
  ##   False - false or null
  ##
  case toLowerAscii(pgStr)
  of "t", "true", "y", "yes", "1", "on":
    true
  else:
    false


template withTx*(db: DbConn, body: untyped): untyped =
  ## Execute code body within a single transaction.
  ##
  ## Commit if no errors and abort if any exception
  ##
  try:
    discard startTx(db)
    body
    discard commitTx(db)
  except:
    discard abortTx(db)
