import db_postgres, os, pg_util, sequtils, strformat, strutils

proc appTables(db: DbConn): seq[Table] =
  result = tables(db).filter(proc(x: Table): bool =
                                 not(x.schema == "pg_catalog" or
                                     x.schema == "information_schema"))

proc createTempSchema(db: DbConn) =
  let schema = "nim_pg_util_test"
  let orig_schemas = schemas(db)
  let orig_tables = appTables(db)
  let tableName = schema & ".test_table"
  try:
    echo "schemas = ", $orig_schemas
    echo &"create schema {schema} = ", createSchema(db, schema)
    echo("  schema was created: ", len(orig_schemas) + 1 == len(schemas(db)))
    echo "schemas = ", $schemas(db)
    echo(&"creating table {tableName} = ",
          tryExec(db, sql(&"create table {tableName}()")))
    echo("  table was created; ", len(orig_tables) + 1 == len(appTables(db)))
  finally:
    echo &"delete schema {schema} = ", deleteSchema(db, schema, true)
    echo("  table was deleted; ", len(orig_tables) == len(appTables(db)))
    echo("  schema was deleted: ", len(orig_schemas) == len(schemas(db)))
    echo "schemas = ", $schemas(db)


proc verifyTables(db: DbConn) =
  let tableList = tables(db)
  let appTables = tableList.filter(proc(x: Table): bool =
                                     not(x.schema == "pg_catalog" or
                                         x.schema == "information_schema"))
  echo "num tables = ", $len(appTables)
  for tbl in appTables:
    echo $tbl

proc verifyCurrent(db: DbConn) =
  echo "current db = ", currentDatabase(db)
  echo "current user = ", currentUser(db)
  echo "schema search path = ", $schemaSearchPath(db)
  echo "current schema = ", currentSchema(db)
  echo "current schemas (no system) = ", currentSchemas(db, false)
  echo "current schemas (w/ system) = ", currentSchemas(db, true)

proc testAlterSearchPath(db: DbConn) =
  let orig = schemaSearchPath(db)
  try:
    echo "altering search_path: ", $setSchemaSearchPath(db,
                                                        @["public", "pub_ext"])
    echo "    path = ", $schemaSearchPath(db)
  finally:
    echo "reverting search_path: ", setSchemaSearchPath(db, orig)
    echo "    path = ", $schemaSearchPath(db)

proc hr() =
  echo repeat('-', 60)

when isMainModule:
  let url = paramStr(1)
  let db = openDb(url)

  try:
    verifyCurrent(db)

    hr()

    testAlterSearchPath(db)

    hr()

    verifyTables(db)

    hr()

    createTempSchema(db)
  finally:
    closeDb(db)
