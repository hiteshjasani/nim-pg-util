import db_postgres, sequtils, times
from strutils import join, toLowerAscii
from postgres import pqfinish, pqreset, pqstatus, CONNECTION_OK

type
  Table* = object
    schema*: string
    name*: string
    owner*: string

var
  psGetCurrentDatabase: SqlPrepared
  psGetCurrentUser: SqlPrepared
  psGetCurrentSchema: SqlPrepared
  psGetCurrentSchemas: SqlPrepared
  psGetSearchPath: SqlPrepared
  psListSchemas: SqlPrepared
  psListTables: SqlPrepared

proc prepareStatements*(db: DbConn) =
  psGetCurrentDatabase = prepare(db, "getCurrentDatabase",
                                 sql("select current_database()"), 0)
  psGetCurrentUser = prepare(db, "getCurrentUser",
                             sql("select current_user"), 0)
  psGetCurrentSchema = prepare(db, "getCurrentSchema",
                               sql("select current_schema"), 0)
  psGetCurrentSchemas = prepare(db, "getCurrentSchemas",
                                sql("select current_schemas($1)"), 1)
  psGetSearchPath = prepare(db, "getSearchPath",
                            sql("show search_path"), 0)
  psListSchemas = prepare(db, "listSchemas",
                          sql("""
select nspname
  from pg_catalog.pg_namespace"""), 0)
  psListTables = prepare(db, "listTables",
                         sql("""
select schemaname,tablename,tableowner
  from pg_catalog.pg_tables"""), 0)

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
  result = open("", "", "", uri)
  prepareStatements(result)

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

proc currentDatabase*(db: DbConn): string =
  getValue(db, psGetCurrentDatabase)

proc currentUser*(db: DbConn): string =
  getValue(db, psGetCurrentUser)

proc currentSchema*(db: DbConn): string =
  getValue(db, psGetCurrentSchema)

proc currentSchemas*(db: DbConn, includeSystemSchemas: bool): string =
  getValue(db, psGetCurrentSchemas, includeSystemSchemas)

proc schemaSearchPath*(db: DbConn): seq[string] =
  ## Postgres default search_path
  ##    "$user", public
  ##
  let val = getValue(db, psGetSearchPath)
  result = strutils.split(val, ",")

proc setSchemaSearchPath*(db: DbConn, schemas: seq[string]): bool =
  ## Set the schema search path.
  ##
  ## Postgres uses a default search path of "$user",public
  ##
  ## Be careful when setting this as you have access to more or fewer
  ## tables than you expect.
  ##
  ## WARNING: Do NOT call this function with any user data as it is
  ## NOT protected against sql injection!
  ##
  # FIXME: This should be a prepared statement but the same syntax
  # won't compile.  But noone should let any user data be passed down
  # to this function unless they are a glutton for punishment and
  # pain.
  let path = join(schemas, ",")
  tryExec(db, sql("set search_path to " & path))

proc schemas*(db: DbConn): seq[string] =
  let rows = getAllRows(db, psListSchemas)
  map(rows, proc(row: Row): string = row[0])

proc createSchema*(db: DbConn, schema: string): bool =
  ## Create a new schema.
  ##
  ## WARNING: Do NOT call this function with any user data as it is
  ## NOT protected against sql injection!
  ##
  # FIXME: This should be a prepared statement but the same syntax
  # won't compile.  But noone should let any user data be passed down
  # to this function unless they are a glutton for punishment and
  # pain.
  tryExec(db, sql("create schema " & schema))

proc deleteSchema*(db: DbConn, schema: string, cascade: bool = false): bool =
  ## Delete the schema
  ##
  ## Specify cascade = true if you want to drop all contained objects
  ##
  ## WARNING: Do NOT call this function with any user data as it is
  ## NOT protected against sql injection!
  ##
  # FIXME: This should be a prepared statement but the same syntax
  # won't compile.  But noone should let any user data be passed down
  # to this function unless they are a glutton for punishment and
  # pain.
  let stat = if cascade: "drop schema " & schema & " cascade"
             else: "drop schema " & schema
  tryExec(db, sql(stat))

proc toTable(row: Row): Table =
  result.schema = row[0]
  result.name = row[1]
  result.owner = row[2]

proc tables*(db: DbConn): seq[Table] =
  ## List tables accessible to the current user
  let rows = getAllRows(db, psListTables)
  map(rows, toTable)

proc deleteTable*(db: DbConn, tableName: string): bool =
  tryExec(db, sql("delete table " & tableName))
