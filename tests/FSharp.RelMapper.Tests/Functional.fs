module FSharp.RelMapper.Tests.Functional

open System
open Expecto
open Expecto.Logging
open Microsoft.Data.Sqlite
open FSharp.RelMapper
open System

let logger = Log.create "Functional"

type DatabaseWrapper() =
    let dbFile = IO.Path.GetTempFileName()
    do logger.logSimple(Message.eventX "Initializing database on {file}" LogLevel.Warn |> Message.setField "file" dbFile)

    let conn = new SqliteConnection($"Data source={dbFile}")
    do 
        conn.Open()
        use cmd = new SqliteCommand("create table test(id bigint not null, description text not null, value real not null, date date null, primary key(id))", conn)
        cmd.ExecuteNonQuery() |> ignore
        use cmd = new SqliteCommand("insert into test values (1, 'this is a test 1', 10.1, null), (2, 'this is another test', 20.0, '2022-06-21')", conn)
        cmd.ExecuteNonQuery() |> ignore

    member __.Conn = conn

    interface IDisposable with
        member self.Dispose() =
            do logger.logSimple(Message.event LogLevel.Warn "Disposing database")
            conn.Close()
            conn.Dispose()
            IO.File.Delete(dbFile)

type Record = 
    {
        Id : int64
        Description : string
        Value : double
    }

type RecordWithDate = 
    {
        Id : int64
        Description : string
        Value : double
        Date : DateTime option
    }

type Param = {id : int64}
    
[<Tests>]
let testSqlite =
    testList "functional tests using a real sqlite connection" [
        let withDb f () = 
            use db = new DatabaseWrapper()
            f db

        yield! testFixture withDb [
            "test simple query with tuple",
            fun db ->
                let res = 
                    db.Conn.Query<int64 * string * float * DateTime option>(
                        "select * from test order by id", 
                        customAdapters = (CustomTypeAdapterMap.Empty |> CustomTypeAdapterMap.add "3" (fun wrapper ->
                            wrapper.GetStringOption("date") |> Option.map DateTime.Parse
                        ))
                    ) 
                    |> List.ofSeq
                Expect.sequenceEqual res [(1, "this is a test 1", 10.1, None); (2, "this is another test", 20.0, (Some (DateTime(2022, 06, 21))))] "Records equal"

            "test simple query with record",
            fun db ->
                let res = db.Conn.Query<Record>("select description as Description, value as Value, id as Id from test order by id") |> List.ofSeq
                Expect.sequenceEqual res [{Id = 1; Description = "this is a test 1"; Value = 10.1}; {Id = 2; Description = "this is another test"; Value = 20.0}] "Records equal"

            "test simple query with record and custom type",
            fun db ->
                /// sqlite3 doesn't have proper support for types so we must pass a type map
                let typeMap = 
                    CustomTypeMap.Empty
                    |> CustomTypeMap.add (fun wrapper i ->
                        DateTime.Parse(wrapper.Reader.GetString(i))
                    )
                    |> CustomTypeMap.add (fun wrapper i ->
                        if wrapper.Reader.[i] = DBNull.Value then
                            None
                        else
                            DateTime.Parse(wrapper.Reader.GetString(i)) |> Some
                    )

                let res = 
                    db.Conn.Query<RecordWithDate>("select description as Description, value as Value, id as Id, date as Date from test order by id", typeMap = typeMap) 
                    |> List.ofSeq
                Expect.sequenceEqual res [{Id = 1; Description = "this is a test 1"; Value = 10.1; Date = None}; {Id = 2; Description = "this is another test"; Value = 20.0; Date = Some (DateTime(2022, 06, 21))}] "Records equal"
    
            "test simple query with parameters from anonymous record",
            fun db ->
                let res = 
                    db.Conn.Query<int64 * string, _>("select * from test where id = :id", {|id = 2|}) 
                    |> List.ofSeq
                Expect.sequenceEqual res [(2L, "this is another test")] "Records equal"

            "test simple query with parameters from record",
            fun db ->
                let res = 
                    db.Conn.Query<int64 * string, _>("select * from test where id = :id", {id = 2}) 
                    |> List.ofSeq
                Expect.sequenceEqual res [(2L, "this is another test")] "Records equal"

            "test simple query with parameters",
            fun db ->
                let res = 
                    db.Conn.Query<int64 * string>("select * from test where id = :id", ParameterList.empty |> ParameterList.add "id" 2) 
                    |> List.ofSeq
                Expect.sequenceEqual res [(2L, "this is another test")] "Records equal"

            "test query single record",
            fun db ->
                let res = db.Conn.QueryOne<Record>("select description as Description, value as Value, id as Id from test where id=1")
                let row = Expect.wantSome res "row is found"
                Expect.equal row {Id = 1; Description = "this is a test 1"; Value = 10.1} "Record equal"

            "test query single record with parameter not found",
            fun db ->
                let res = db.Conn.QueryOne<Record, _>("select description as Description, value as Value, id as Id from test where id=:id", {|id = 1000|})
                Expect.isNone res "Row not found"

        ]
    ]
