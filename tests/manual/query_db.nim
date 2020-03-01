import os, pg_util

when isMainModule:
  let url = paramStr(1)
  let db = openDb(url)

  try:
    echo "current db = ", currentDatabase(db)
    echo "current user = ", currentUser(db)
    echo "schema search path = ", $schemaSearchPath(db)
    echo "current schema = ", currentSchema(db)
    echo "current schemas (no system) = ", currentSchemas(db, false)
    echo "current schemas (w/ system) = ", currentSchemas(db, true)
    echo "altering search_path: ",
      $setSchemaSearchPath(db, @["public", "pub_ext"])
    echo "schema search path = ", $schemaSearchPath(db)
    let tableList = tables(db)
    echo "num tables = ", $len(tableList)
    for tbl in tableList:
      echo $tbl
  finally:
    closeDb(db)
