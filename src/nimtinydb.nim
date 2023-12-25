## =================
## Module docstrings
## =================
## :ModuleName: Nim TinyDB
## :Author: Harrison Mbemba
## :License: MIT
## :Version: 0.1
##
## Introduction
## ------------
## This module provides a simple implementation of a TinyDB, which is a lightweight, file-based JSON database. It allows creating a new database or opening an existing one. The module provides various functions for inserting, retrieving, updating, and deleting data from the database.

include prelude
import std/json
import typetraits
import options, jsony
import icecream
import pathutils

export options, jsony, json, pathutils

##################
# Types
##################
type
    InvalidIDError*          = object of Exception
    InvalidQueryValueError*  = object of Exception
    UnknownQueryError*       = object of Exception
    UnknownQueryValueError*  = object of Exception
    TableNotFoundError*      = object of Exception

    tinydb* = object
      ## Overview
      ## 
      ## - This is the main object of the module. It is used to create a new database or to open an existing one.
      ## 
      ## Notes
      ##  - Do not instantiate this object directly. Use the newTinyDB() proc instead.
      ##  - The database file is a JSON file.
      path*  : strictfile
      table* : string
    
    basic_query_types   = enum 
      equals_query      ,
      contains_query    ,
      not_equals_query  ,
      exists_query      ,
      in_query          ,
      test_query        ,
      substring_query

    compound_query_types = enum
      and_query          ,
      or_query           ,
    
    basic_query        = object 
      field            : string
      query_type       : basic_query_types
      value            : JsonNode

    compound_query     = object 
      queries          : seq[basic_query]
      compound_queries : seq[compound_query]
      query_type       : compound_query_types

    queryField         = distinct string
    
    doc_id             =  int



##################
# Internal procs
##################

proc getHighestId(dbTable:JsonNode): int =

  const highest_id_field_name = "highest_id"

  # if the meta object does not exist in the db table
  # get the highest id by array iteration
  # then create the meta object
  # and return the highest id
  if not dbTable.contains("meta"):
    var highestId = 0

    for key, value in dbTable:
      let id = parseInt(key)
      if id > highestId:
        highestId = id

    dbTable.add("meta",%*{highest_id_field_name: %highestId})
    return highestId

  
  # if the meta object exists in the db table
  # get the highest id from the meta object
  # and return it
  return dbTable["meta"][highest_id_field_name].getInt()

proc update_highest_id(dbTable: JsonNode, new_highest_id:int) = 
  dbTable["meta"]["highest_id"] = %new_highest_id

proc toStr*(t:JsonNode):string = $t

proc get_not_unique(input:seq[int]): seq[int] =
  var ctable = initCountTable[int](input.len)
  for id in input:
    ctable.inc(id)
  result = @[]
  for val, count in ctable:
    if count > 1:
      result.add(val)

proc get_unique(input:seq[int]): seq[int] =
  var ctable = initCountTable[int](input.len)
  for id in input:
    ctable.inc(id)
  for val, count in ctable:
    if count == 1:
      result.add(val)

proc keep_duplicates[T](
        search_results : seq[tuple[item: T, doc_id: int]] ,
        schema         : typedesc[T],
    ): seq[tuple[item: T, doc_id: int]] =

  # Now we need to find the common doc_ids in all the results
  # Turn [(JN,1), (JN,2), (JN,3), (JN,4), (JN,3),(JN,4),(JN,5)] into [(JN,3), (JN,4)]
  let (_,doc_ids) = search_results.unzip
  let final = search_results.filterIt(it.doc_id in doc_ids.get_not_unique).deduplicate
  return final

##################
# External procs
##################

## Constructors ##
##################

# let db = newTinyDB("/root/db.json")
# let db = newTinyDB("/root/db.json", "myTable")
proc newTinyDB*(pathToJsonDB: string, tableName:string = ""): tinydb = 
  ## Overview
  ## 
  ##  ...
  ## 
  ## Parameters
  ## 
  ##  pathToJsonDB: string
  ##      ...
  ## 
  ## Returns
  ## 
  ##  tinydb
  ## 
  ## Example Usage
  ## 
  ## ```
  ## var db = newTinyDB("db.json")
  ## ```
  ## 
  ## Raises
  ##  ...
  ## 
  ## Notes
  ##  ...

  const defaultTableName = "_default"
  
  let user_specified_table:tuple[exists:bool,val:string] = 
    if tableName.len > 0: 
      (exists:true, val:tableName) 
    else: 
      (exists:false, val:"")
  
  # If the db json file exists ...
  if pathToJsonDB.fileExists:
    
    # If the user has not specified a table name, just return the default table
    if not user_specified_table.exists:
      return tinydb(path: newStrictFile(pathToJsonDB), table:defaultTableName )
    

    # If the user has specified a table name, check if it exists in the db
    let jsonData = parseFile(pathToJsonDB)
    
    # if the user_specified_table exists in the db, return the tinydb object with their user_specified_table
    if jsonData.contains(user_specified_table.val):      
      return tinydb(path: newStrictFile(pathToJsonDB), table:user_specified_table.val )

    # if user_specified_table does not exist in the db, create the table and return the db
    else:
      
      let node     = %*{"highest_id":0}
      jsonData.add(user_specified_table.val,%*{"meta":node})
      writeFile(pathToJsonDB, pretty(jsonData))

      return tinydb(path: newStrictFile(pathToJsonDB), table:user_specified_table.val )

  # if db json file does not exist, create it
  if not pathToJsonDB.fileExists:
    proc buildTable(): string = 
      # If the user has specified a table name, create the table
      if user_specified_table.exists:
        return ""","__TABLE__":{"meta":{"highest_id":0}}""".replace("__TABLE__", user_specified_table.val)
      else:
        return ""
    
    let content = 
      """
{
  "__defaultTableName__": { "meta":{"highest_id":0}

  }__BUILD_TABLE__
  
}
      """.multiReplace(@[
          ("__BUILD_TABLE__", buildTable()), 
          ("__defaultTableName__", defaultTableName)
          ])

    return tinydb(path: newStrictFile(
                                      pathToJsonDB, 
                                      mkIfNotExist  = true,
                                      content       = content, 
                                      allowed_ext   = @["json"],
                                  ),
              table: if user_specified_table.exists: user_specified_table.val else: defaultTableName
            )

# Where("tags")
proc Where*(queryString: string) : queryField = queryField(queryString)


## Query Procs ###
##################

  # Basic queries
##################

# db.search(Where('done') ==  %true  )
proc `==`*(field: queryField, b: JsonNode): basic_query =
    basic_query(
              field      : field.string , 
              value      : b            ,           
              query_type : equals_query
            )


# db.search(Where('name') !=  %"John")
proc `!=`*(field: queryField, b: JsonNode): basic_query =  
    basic_query(
              field      : field.string , 
              value      : b            ,           
              query_type : not_equals_query
            )

# db.search(Where('name').exists)
proc exists*(field: queryField): basic_query =  
    basic_query(
              field      : field.string ,
              query_type : exists_query
            )

# db.search(Where('name').in(%["John", "Doe"]))
# db.search(Where('id').in(song_ids))
proc `in`*(field: queryField, b: JsonNode): basic_query =  
    if b.kind == JArray:
      basic_query(
                field      : field.string , 
                value      : b            ,           
                query_type : in_query
              )
    else:
      raise newException(InvalidQueryValueError, "The RHS of the 'in' query must be a JArray")


# db.search(Where('student_ids').contains(42))
# Field must be a JArray
proc contains*(field: queryField, b: JsonNode): basic_query =  
    basic_query(
              field      : field.string , 
              value      : b            ,           
              query_type : contains_query
            )

# db.search(Where('name').has_substr("Joh"))
# Field must be a JString
proc has_substr*(field: queryField, b: JsonNode): basic_query =  
    basic_query(
              field      : field.string , 
              value      : b            ,           
              query_type : substring_query
            )

##################### Compound queries ###################

# db.search((Where("id") == %"123") & (Where("genre") == %"rap"))
proc `&`*(a, b: basic_query): compound_query =
  return compound_query(
          queries           : @[a, b]         , 
          query_type        : and_query
        )

# db.search((Where("id") == %"123") & (Where("genre") == %"rap") & (Where("name") == %"Money in the Bank"))
proc `&`*(a: compound_query, b: basic_query): compound_query =
  return compound_query(
          queries           : @[b], 
          compound_queries  : @[a],   
          query_type        : and_query
        )

# db.search((Where("id") == %"123") | (Where("genre") == %"rap"))
proc `|`*(a, b: basic_query): compound_query =
  return compound_query(
          queries           : @[a, b]         , 
          query_type        : or_query
        )

# db.search(( (Where("id") == %"123") & (Where("genre") == %"rap") & (Where("name") == %"Money in the Bank") ) | ( Where("name") == %"Is This Love") )
proc `|`*(a: compound_query, b: basic_query): compound_query =
  return compound_query(
          queries           : @[b], 
          compound_queries  : @[a],   
          query_type        : or_query
        )

# Create
##################

proc private_insert(database: JsonNode, table_name, db_path: string, data: seq[JsonNode]): tuple[ok:bool, err:string, doc_id:Option[seq[int]]] =  
  try:
      if not database.contains(table_name):
        raise newException( TableNotFoundError,"Table not found: " & table_name)

      var newly_inserted_ids = newSeq[int]()
      for datum in data:
        let next_id = getHighestId(database[table_name]) 
      
        # Insert the new data
        database[table_name][$next_id] = datum

        let new_next_id = next_id + 1

        # Update the highest_id field
        database[table_name].update_highest_id(new_next_id)

        newly_inserted_ids.add(next_id)

      writeFile(db_path, pretty(database))

      return (true, "", some(newly_inserted_ids))
  except CatchableError:
    let
      e       = getCurrentException()
      msg     = getCurrentExceptionMsg()
      fullMsg = repr(e) & ": " & msg
    return (false, fullMsg, none(seq[int]))

# Insert a single item into the database
proc insert*(db: tinydb, data: JsonNode): tuple[ok:bool, err:string, doc_id:Option[int]] =
  ## Example
  ## -------
  ## var data = %*{
  ##   "description": "new task",
  ##   "tags": "",
  ##   "project": "newProject",
  ##   "quarter": "1",
  ##   "notes": [],
  ##   "script": "script.py"
  ## }
  ## 
  ## discard db.insert(data)
  try:
      let database = parseFile($db.path)
      let insert_req = private_insert(database, db.table, $db.path, @[data])
      if not insert_req.ok:
        return (false, insert_req.err, none(int))
      return (true, "", some(insert_req.doc_id.get[0]))
  except CatchableError:
    let
      e       = getCurrentException()
      msg     = getCurrentExceptionMsg()
      fullMsg = repr(e) & ": " & msg
    return (false, fullMsg, none(int))

# Insert multiple items into the database
proc insert*(db: tinydb, data: seq[JsonNode]): tuple[ok:bool, err:string, doc_ids:Option[seq[int]]] = 
  ## Example
  ## -------
  ## var data = @[
  ##   %*{
  ##     "description": "new task",
  ##     "tags": "",
  ##   },
  ##   %*{
  ##     "description": "new task",
  ##     "tags": "",
  ##   }
  ## ]
  ##
  ## discard db.insert(data)
  
  try:
      let database = parseFile($db.path)
      let insert_req = private_insert(database, db.table, $db.path, data)
      if not insert_req.ok:
        return (false, insert_req.err, none(seq[int]))
      return (true, "", some(insert_req.doc_id.get))
  except CatchableError:
    let
      e       = getCurrentException()
      msg     = getCurrentExceptionMsg()
      fullMsg = repr(e) & ": " & msg
    return (false, fullMsg, none(seq[int]))

# Search the database
##################

proc handleEqualsQuery[T](
        schema         : typedesc[T],
        database       : JsonNode,
        table_name     : string,
        query          : basic_query,
    ): Option[seq[tuple[item: T, doc_id: int]] ] =
    # [(item, doc_id), (item, doc_id), (item, doc_id)]
    
    var match_results = 
      when schema is JsonNode:
        newSeq[JsonNode]()
      else:
        newSeq[T]()
    var doc_ids       = newSeq[int]()
    for id, data in database[table_name].pairs():
        if id != "meta" and data.contains(query.field):
          if data[query.field] == query.value:
              doc_ids.add(id.parseInt())
              when schema is JsonNode:
                  match_results.add(data)
              else:
                  match_results.add(($data).fromJson(schema))
    if match_results.len > 0:
      return some(zip(match_results, doc_ids))
    return none(seq[(T, int)])

proc handleNotEqualsQuery[T](
        schema         : typedesc[T],
        database       : JsonNode,
        table_name     : string,
        query          : basic_query,
    ): Option[seq[tuple[item: T, doc_id: int]] ] =
    # [(item, doc_id), (item, doc_id), (item, doc_id)]
    
    var match_results = 
      when schema is JsonNode:
        newSeq[JsonNode]()
      else:
        newSeq[T]()
    var doc_ids       = newSeq[int]()
    for id, data in database[table_name].pairs():
        if id != "meta" and data.contains(query.field):
          if data[query.field] != query.value:
              doc_ids.add(id.parseInt())
              when schema is JsonNode:
                  match_results.add(data)
              else:
                  match_results.add(($data).fromJson(schema))
    if match_results.len > 0:
      return some(zip(match_results, doc_ids))
    return none(seq[(T, int)])

proc handleContainsQuery[T](
        schema         : typedesc[T],
        database       : JsonNode,
        table_name     : string,
        query          : basic_query,
    ): Option[seq[tuple[item: T, doc_id: int]] ] =
    # [(item, doc_id), (item, doc_id), (item, doc_id)]
    
    var match_results = 
      when schema is JsonNode:
        newSeq[JsonNode]()
      else:
        newSeq[T]()
    var doc_ids       = newSeq[int]()
    for id, data in database[table_name].pairs():
        if id != "meta" and data.contains(query.field):
          if data[query.field].kind == JArray:
            if %query.value in data[query.field]:
                doc_ids.add(id.parseInt())
                when schema is JsonNode:
                    match_results.add(data)
                else:
                    match_results.add(($data).fromJson(schema))
    if match_results.len > 0:
      return some(zip(match_results, doc_ids))
    return none(seq[(T, int)])

proc handleHasSubstr[T](
        schema         : typedesc[T],
        database       : JsonNode,
        table_name     : string,
        query          : basic_query,
    ): Option[seq[tuple[item: T, doc_id: int]] ] =
    # [(item, doc_id), (item, doc_id), (item, doc_id)]
    
    var match_results = 
      when schema is JsonNode:
        newSeq[JsonNode]()
      else:
        newSeq[T]()
    var doc_ids       = newSeq[int]()
    for id, data in database[table_name].pairs():
        if id != "meta" and data.contains(query.field):
          if data[query.field].kind == JString:
            if data[query.field].getStr.contains(query.value.getStr):
                doc_ids.add(id.parseInt())
                when schema is JsonNode:
                    match_results.add(data)
                else:
                    match_results.add(($data).fromJson(schema))
    if match_results.len > 0:
      return some(zip(match_results, doc_ids))
    return none(seq[(T, int)])

proc handleExistsQuery[T](
        schema         : typedesc[T],
        database       : JsonNode,
        table_name     : string,
        query          : basic_query,
    ): Option[seq[tuple[item: T, doc_id: int]] ] =
    # [(item, doc_id), (item, doc_id), (item, doc_id)]
    
    var match_results = 
      when schema is JsonNode:
        newSeq[JsonNode]()
      else:
        newSeq[T]()
    var doc_ids       = newSeq[int]()
    for id, data in database[table_name].pairs():
        if id != "meta" and data.contains(query.field):
            doc_ids.add(id.parseInt())
            when schema is JsonNode:
                match_results.add(data)
            else:
                match_results.add(($data).fromJson(schema))
    if match_results.len > 0:
      return some(zip(match_results, doc_ids))
    return none(seq[(T, int)])

proc handleInQuery[T](
        schema         : typedesc[T],
        database       : JsonNode,
        table_name     : string,
        query          : basic_query,
    ): Option[seq[tuple[item: T, doc_id: int]] ] =
    # [(item, doc_id), (item, doc_id), (item, doc_id)]
    
    var match_results = 
      when schema is JsonNode:
        newSeq[JsonNode]()
      else:
        newSeq[T]()
    var doc_ids       = newSeq[int]()
    for id, data in database[table_name].pairs():
        if id != "meta" and data.contains(query.field):
          if query.value.kind == JArray:
            if data[query.field] in %query.value:
                doc_ids.add(id.parseInt())
                when schema is JsonNode:
                    match_results.add(data)
                else:
                    match_results.add(($data).fromJson(schema))
    if match_results.len > 0:
      return some(zip(match_results, doc_ids))
    return none(seq[(T, int)])

proc basic_query_search*[T](database: JsonNode, query: basic_query, schema:typedesc[T], table_name:string): tuple[ok:bool, err:string, vals:Option[seq[tuple[item: T, doc_id: int]]]] =
    try:
      case query.query_type:
        of equals_query: 
          when schema is typeof(nil):
            return (true, "", handleEqualsQuery(JsonNode, database, table_name, query))
          else:
            return (true, "", handleEqualsQuery(schema, database, table_name, query))
        of contains_query:
          when schema is typeof(nil):
            return (true, "", handleContainsQuery(JsonNode, database, table_name, query))
          else:
            return (true, "", handleContainsQuery(schema, database, table_name, query))
        of not_equals_query:
          when schema is typeof(nil):
            return (true, "", handleNotEqualsQuery(JsonNode, database, table_name, query))
          else:
            return (true, "", handleNotEqualsQuery(schema, database, table_name, query))
        of exists_query:
          when schema is typeof(nil):
            return (true, "", handleExistsQuery(JsonNode, database, table_name, query))
          else:
            return (true, "", handleExistsQuery(schema, database, table_name, query))
        of in_query:
          when schema is typeof(nil):
            return (true, "", handleInQuery(JsonNode, database, table_name, query))
          else:
            return (true, "", handleInQuery(schema, database, table_name, query))
        of substring_query:
          when schema is typeof(nil):
            return (true, "", handleHasSubstr(JsonNode, database, table_name, query))
          else:
            return (true, "", handleHasSubstr(schema, database, table_name, query))
        else:
          raise newException(UnknownQueryError, "The query is not supported")



    except CatchableError:
      let
        e   = getCurrentException()
        msg = getCurrentExceptionMsg()
        fullMsg = repr(e) & ": " & msg
      return (false, fullMsg, none(seq[(schema, int)]) )

proc handle_compound_queries[T](
        schema         : typedesc[T],
        database       : JsonNode,
        table_name     : string,
        compoundQuery  : compound_query
    ): Option[seq[tuple[item: T, doc_id: int]]] =

    var results: seq[tuple[item: T, doc_id: int]]   
    # Process basic queries first
    for q in compoundQuery.queries:
        let search = basic_query_search(database, q, schema, table_name)
        if search.vals.isSome():
            for res in search.vals.get():
                results.add(res)

    # Process nested compound queries
    for nestedQuery in compoundQuery.compound_queries:
        let nestedResults = handle_compound_queries(schema, database, table_name, nestedQuery)
        if nestedResults.isSome():
            case nestedQuery.query_type:
                of and_query:
                    results = results & nestedResults.get.keep_duplicates(schema)
                of or_query:
                    results = results & nestedResults.get().deduplicate
    if results.len > 0:
        return some(results)
    else:
        return none(seq[(T, int)])


## BASIC QUERY SEARCH ###
########################


# db.search(Where('name') == "John")
# db.search(Where('name') != "John")
# db.search(Where('name').exists)
# db.search(Where('name').in(["John", "Doe"]))
# db.search(Where('name').contains("Joh"))
# db.search(Where('student_ids').contains(42))
proc search*(
              db     : tinydb, 
              query  : basic_query,
              schema : typedesc = nil.typeof
            ): tuple[ok:bool, err:string, vals:Option[seq[tuple[item: JsonNode, doc_id: int]] ] ] =

  try:
      let database = parseFile($db.path)
      basic_query_search(database, query, JsonNode, db.table)
  except CatchableError:
    let
      e   = getCurrentException()
      msg = getCurrentExceptionMsg()
      fullMsg = repr(e) & ": " & msg
    return (false, fullMsg, none(seq[(JsonNode, int)]) )


# db.search(Task, Where('name') == "John")
# db.search(Task, Where('name') != "John")
# db.search(Task, Where('name').exists)
# db.search(Task, Where('name').in(["John", "Doe"]))
# db.search(Task, Where('name').contains("Joh"))
# db.search(Task, Where('student_ids').contains(42))
proc search*(
              db     : tinydb,
              schema : typedesc,
              query  : basic_query,
              
            ): tuple[ok:bool, err:string, vals:Option[seq[tuple[item: schema, doc_id: int]] ] ] =

  try:
      let database = parseFile($db.path)
      basic_query_search(database, query, schema, db.table)
  except CatchableError:
    let
      e   = getCurrentException()
      msg = getCurrentExceptionMsg()
      fullMsg = repr(e) & ": " & msg
    return (false, fullMsg, none(seq[(schema, int)]) )

## COMPOUND QUERY SEARCH ###
############################

# db.search(Where('name') == "John" & Where('done') == true)
# db.search(Where('name') contains "John" | Where('done') == true)
proc search*(
              db    : tinydb, 
              query : compound_query
            ): tuple[ok:bool, err:string, vals:Option[seq[tuple[item: JsonNode, doc_id: int]] ] ] =
    try:
      let database = parseFile($db.path)
      case query.query_type:
        of and_query: 
          let search_results = handle_compound_queries(JsonNode, database, db.table, query)
          if search_results.isSome():
            let final = search_results.get.keep_duplicates(JsonNode)
            if final.len > 0:
              return (true, "", some(final))
            else:
              return (true, "", none(seq[(JsonNode, int)]) )
          else:
            return (true, "", none(seq[(JsonNode, int)]) )

        of or_query: 
          var search_results = handle_compound_queries(JsonNode, database, db.table, query)
          if search_results.isSome():
            return (true, "", some(search_results.get.deduplicate))
          else:
            return (true, "", none(seq[(JsonNode, int)]) )

    except CatchableError:
      let
        e   = getCurrentException()
        msg = getCurrentExceptionMsg()
        fullMsg = repr(e) & ": " & msg
      return (false, fullMsg, none(seq[(JsonNode, int)]) )

# db.search(Task, Where('name') == "John" & Where('done') == true)
# db.search(Task, Where('name') contains "John" | Where('done') == true)
proc search*(
              db    : tinydb, 
              schema: typedesc,
              query : compound_query
            ): tuple[ok:bool, err:string, vals:Option[seq[tuple[item: schema, doc_id: int]] ] ] =
    try:
      let database = parseFile($db.path)
      case query.query_type:
        of and_query: 
          let search_results = handle_compound_queries(schema, database, db.table, query)
          if search_results.isSome():
            let final = search_results.get.keep_duplicates(schema)
            if final.len > 0:
              return (true, "", some(final))
            else:
              return (true, "", none(seq[(schema, int)]) )
          else:
            return (true, "", none(seq[(schema, int)]) )

        of or_query: 
          var search_results = handle_compound_queries(schema, database, db.table, query)
          if search_results.isSome():
            return (true, "", some(search_results.get.deduplicate))
          else:
            return (true, "", none(seq[(schema, int)]) )

    except CatchableError:
      let
        e   = getCurrentException()
        msg = getCurrentExceptionMsg()
        fullMsg = repr(e) & ": " & msg
      return (false, fullMsg, none(seq[(schema, int)]) )

# Read
##################

# db.get(1)
proc get*(db:tinydb, doc_id: int): tuple[ok:bool, err:string, val:Option[tuple[item:JsonNode, doc_id:int]]] = 
  try:
    let database = parseFile($db.path)

    let answer = database[db.table][$doc_id]
    return (true, "", some((answer, doc_id)))
  except CatchableError:
    let
      e   = getCurrentException()
      msg = getCurrentExceptionMsg()
      fullMsg = repr(e) & ": " & msg
    (false, fullMsg, none((JsonNode, int)))

# db.get(where("tags") == "test")
# db.get((where("tags") == "test") & (where("done") == true))
proc get*(
            db    : tinydb, 
            query : basic_query | compound_query
          ): tuple[ok:bool, err:string, val:Option[tuple[item:JsonNode, doc_id:int]]] =
  try:
      let search_req = db.search(query)
      if not search_req.ok:
        return (false, search_req.err, none((JsonNode, int)))

      if search_req.vals.isNone:
        return (true, "", none((JsonNode, int)))
      else:
          ( true, "", some( search_req.vals.get[0] ))
  except CatchableError:
    let
      e   = getCurrentException()
      msg = getCurrentExceptionMsg()
      fullMsg = repr(e) & ": " & msg
    (false, fullMsg, none((JsonNode, int)))

# db.get(Task, where("tags") == "test")
# db.get(Task, (where("tags") == "test") & (where("done") == true))
proc get*(
            db    : tinydb, 
            schema : typedesc,
            query : basic_query | compound_query
          ): tuple[ok:bool, err:string, val:Option[tuple[item:schema, doc_id:int]]] =
  try:
      let search_req = db.search(schema, query)
      if not search_req.ok:
        return (false, search_req.err, none((schema, int)))

      if search_req.vals.isNone:
        return (true, "", none((schema, int)))
      else:
          ( true, "", some( search_req.vals.get[0] ))
  except CatchableError:
    let
      e   = getCurrentException()
      msg = getCurrentExceptionMsg()
      fullMsg = repr(e) & ": " & msg
    (false, fullMsg, none((schema, int)))

# db.upsert({"done": true}, where("tags") == "test")
# db.upsert({"done": true}, (where("tags") == "test") & (where("done") == true))
proc upsert*(
                    db     : tinydb, 
                    data   : JsonNode, 
                    query  : basic_query | compound_query
                ): tuple[ok:bool, err:string, doc_id:Option[int]] = 
  let get_req = db.get(query)
  if not get_req.ok:
    return (false, get_req.err, none(int))
  if get_req.val.isNone:
    return db.insert(data)
  else:
    let update_req = db.update(data, @[get_req.val.get.doc_id])
    if not update_req.ok:
      return (false, update_req.err, none(int))
    else:
      return (true, "", some(get_req.val.get.doc_id))

# let p = db.get(1)
# let o = p.asObj(Task)
proc asObj*[T](jsonNode:JsonNode, schema: typedesc[T]) : T =
  result = jsonNode.toStr.fromJson(schema)

# db.allRaw()
proc allRaw*(db:tinydb, table = "") : JsonNode =
  let database = parseFile($db.path)
  if table == "":
    return database
  else:
    return database[table]

# db.all()
proc all*(db:tinydb) : seq[JsonNode] =
  let database = parseFile($db.path)
  
  # Each key is the doc_id
  # Each value is an entry in that table as a JsonNode
  for k,v in database[db.table]:
    if k != "meta":
        # Add the doc_id to the JsonNode
        var p = copy(v)
        p.add("doc_id", %k)

        # Implicitly return a sequence of JsonNodes
        result.add(p)

# db.all(Task)
proc all*[T](db:tinydb, schema:typedesc[T]) : seq[T] =
  for item in db.all():
    let item_str = $item 
    result.add(item_str.fromJson(schema))

# Update the database
##################

# db.update({"done": true}, @[1, 2, 3])
proc update*(
              db:tinydb, 
              data:JsonNode, 
              doc_ids:seq[int]
            ): tuple[ok:bool, err:string, val:Option[seq[int]]] = 
  if doc_ids.len == 0:
    return (true, "", none(seq[int]))
   

  try:
    let database = parseFile($db.path)

    for id in doc_ids:
      for k, v in data:
        try:
          database[db.table][$id][k] = v
        except KeyError:
          return (false, fmt"The doc_id at {db.table}:{id} does not exist in the database", none(seq[int]))
    writeFile($db.path, pretty(database))
    return (true, "", some(doc_ids))
  except CatchableError:
    let
      e   = getCurrentException()
      msg = getCurrentExceptionMsg()
      fullMsg = repr(e) & ": " & msg
    return (false, fullMsg, none(seq[int]))

# db.update({"done": true}, @[1, 2, 3])
# If a doc_id does not exist, it is ignored
proc update_loose*(
              db      : tinydb, 
              data    : JsonNode, 
              doc_ids : seq[int]
            ): tuple[ok:bool, err:string, val:Option[seq[int]]] = 
  if doc_ids.len == 0:
    return (true, "", none(seq[int]))


  try:
  
    let database = parseFile($db.path)

    var inner_doc_ids = doc_ids
    for id in inner_doc_ids:
      if database[db.table].contains($id):
        for k, v in data:
          try:
            database[db.table][$id][k] = v
          except KeyError:
            inner_doc_ids.del(id)
            continue
    writeFile($db.path, pretty(database))
    return (true, "", some(inner_doc_ids))
  except CatchableError:
    let
      e   = getCurrentException()
      msg = getCurrentExceptionMsg()
      fullMsg = repr(e) & ": " & msg
    return (false, fullMsg, none(seq[int]))

# db.update({"done": true}, where("tags") == "test")
proc update*(
                db     : tinydb, 
                data   : JsonNode, 
                query  : basic_query | compound_query
            ):  tuple[ok:bool, err:string, val:Option[seq[int]]] = 
  let search_req = search(db, query)
  if not search_req.ok:
    return (false, search_req.err, none(seq[int]))
  if search_req.vals.isNone:
    return (true, "", none(seq[int]))

  var doc_ids_to_update: seq[int]
  for item in search_req.vals.get:
    doc_ids_to_update.add(item.doc_id)
  db.update(data, doc_ids_to_update)
  

# Remove a document from the database
##################
proc rm*(db: tinydb, doc_id: int): tuple[ok:bool, err:string, val:Option[int]] =
  try:
    let database = parseFile($db.path)
    database[db.table].delete($doc_id)
    
    let next_id = getHighestId(database[db.table]) 
    let new_next_id = next_id - 1
    database[db.table].update_highest_id(new_next_id)

    writeFile($db.path, pretty(database))
    return (true, "", some(doc_id))
  except CatchableError:
    let
      e   = getCurrentException()
      msg = getCurrentExceptionMsg()
      fullMsg = repr(e) & ": " & msg
    return (false, fullMsg, none(int))

proc rm*(db: tinydb, doc_ids: seq[int]): tuple[ok:bool, err:string, val:Option[seq[int]]] =
  if doc_ids.len == 0:
    return (true, "", none(seq[int]))

  try:
    for doc_id in doc_ids:
      discard db.rm(doc_id)
    return (true, "", some(doc_ids))
  except CatchableError:
    let
      e   = getCurrentException()
      msg = getCurrentExceptionMsg()
      fullMsg = repr(e) & ": " & msg
    return (false, fullMsg, none(seq[int]))


# TODO: Finish implementation that works with 
# <tinydb, tuple[queries: seq[query], query_type: type_of_query]>
# proc count*(db: tinydb, query: query, tableName:string=""): int =
#   let table = if tableName.len > 0: tableName else: "_default"
#   let ids = search(db, query)
#   return ids.len

proc truncate*(db:tinydb) =
    let content = """{
  "_default": {
  }
}"""
    writeFile($db.path, content)
