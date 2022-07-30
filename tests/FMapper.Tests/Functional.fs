module FMapper.Tests.Functional

open System
open Expecto
open Expecto.Logging
open Microsoft.Data.Sqlite
open FMapper
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
        use cmd = new SqliteCommand("create table test_empty(id bigint not null, description text not null, value real not null, date date null, primary key(id))", conn)
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

            "test count query",
            fun db ->
                let res = db.Conn.QueryOne<int64>("select count(*) as n from test")
                let res' = Expect.wantSome res "count(*) is null"
                Expect.equal res' 2L "count(*)"
    
            "test count query empty table",
            fun db ->
                let res = db.Conn.QueryOne<int64>("select count(*) as n from test_empty", exactlyOne = true)
                let res' = Expect.wantSome res "count(*) is null"
                Expect.equal res' 0L "count(*)"

            "test insert with parameters",
            fun db -> 
                let pars =
                    ParameterList.empty
                    |> ParameterList.add "Id" 1000L
                    |> ParameterList.add "Description" "test insert with parameters"
                    |> ParameterList.add "Value" 0.0
                    |> ParameterList.add "Date" (Some (DateTime(2022, 7, 27)))

                let query = "insert into test values (@Id, @Description, @Value, @Date) returning id"
                let res = db.Conn.QueryOne<int64>(query, pars, exactlyOne=true)
                Expect.equal res (Some 1000L) "returning"

                let res = db.Conn.QueryOne<RecordWithDate>("select description as Description, value as Value, id as Id, date as Date from test where id=1000", typeMap = typeMap, exactlyOne = true)
                let row = Expect.wantSome res "row is found"
                Expect.equal row {Id = 1000L; Description = "test insert with parameters"; Value = 0.0; Date = Some (DateTime(2022, 7, 27))} "Record equal"
        
            "test insert with record",
            fun db -> 
                let r =
                    {
                        Id = 2000L
                        Description = "test insert with record"
                        Value = 0.0
                        Date = Some (DateTime(2022, 7, 27))
                    }
                let query = "insert into test values (@Id, @Description, @Value, @Date) returning id"
                let res = db.Conn.QueryOne<int64, _>(query, r, exactlyOne=true)
                Expect.equal res (Some 2000L) "returning"

                let res = db.Conn.QueryOne<RecordWithDate>("select description as Description, value as Value, id as Id, date as Date from test where id=2000", typeMap = typeMap, exactlyOne = true)
                let row = Expect.wantSome res "row is found"
                Expect.equal row {Id = 2000L; Description = "test insert with record"; Value = 0.0; Date = Some (DateTime(2022, 7, 27))} "Record equal"

            "test query with 2 tables",
            fun db ->
                let res = 
                    db.Conn.Query<Record, Record>(
                        "select 
                        t1.description as Description, t1.value as Value, t1.id as Id,
                        t2.description as t2_Description, t2.value as t2_Value, t2.id as t2_Id
                        from test t1
                        join test t2 on t1.id = t2.value / 10 - 1
                        order by t1.id",
                        col2Strategy = Prefix "t2_"

                    ) 
                    |> List.ofSeq
                Expect.sequenceEqual res [{Id = 1; Description = "this is a test 1"; Value = 10.1}, {Id = 2; Description = "this is another test"; Value = 20.0}] "Records equal"

            "test query with 3 tables",
            fun db ->
                let res = 
                    db.Conn.Query<Record, Record, Record>(
                        "select 
                        t1.description as Description, t1.value as Value, t1.id as Id,
                        t2.description as t2_Description, t2.value as t2_Value, t2.id as t2_Id,
                        t3.description as t3_Description, t3.value as t3_Value, t3.id as t3_Id
                        from test t1
                        join test t2 on t1.id = t2.value / 10 - 1
                        join test t3 on t1.id = t3.id and t3.id < 2
                        order by t1.id",
                        col2Strategy = Prefix "t2_",
                        col3Strategy = Prefix "t3_"
                    ) 
                    |> List.ofSeq
                Expect.sequenceEqual 
                    res 
                    [
                        {Id = 1; Description = "this is a test 1"; Value = 10.1}, {Id = 2; Description = "this is another test"; Value = 20.0}, {Id = 1; Description = "this is a test 1"; Value = 10.1}
                    ] "Records equal"

            "test query with 4 tables",
            fun db ->
                let res = 
                    db.Conn.Query<Record, Record, Record, RecordWithDate>(
                        "select 
                        t1.description as Description, t1.value as Value, t1.id as Id,
                        t2.description as t2_Description, t2.value as t2_Value, t2.id as t2_Id,
                        t3.description as t3_Description, t3.value as t3_Value, t3.id as t3_Id,
                        t4.description as t4_Description, t4.value as t4_Value, t4.id as t4_Id, t4.Date as t4_Date
                        from test t1
                        join test t2 on t1.id = t2.value / 10 - 1
                        join test t3 on t1.id = t3.id and t3.id < 2
                        join test t4 on t1.id = t4.id - 1
                        order by t1.id",
                        col2Strategy = Prefix "t2_",
                        col3Strategy = Prefix "t3_",
                        col4Strategy = Prefix "t4_",
                        typeMap = typeMap
                    ) 
                    |> List.ofSeq
                Expect.sequenceEqual 
                    res 
                    [
                        {Id = 1; Description = "this is a test 1"; Value = 10.1}, 
                        {Id = 2; Description = "this is another test"; Value = 20.0}, 
                        {Id = 1; Description = "this is a test 1"; Value = 10.1},
                        {Id = 2; Description = "this is another test"; Value = 20.0; Date = (Some (DateTime(2022, 6, 21)))}
                    ] "Records equal"

            "test query with 5 tables",
            fun db ->
                let res = 
                    db.Conn.Query<Record, Record, Record, RecordWithDate, Record>(
                        "select 
                        t1.description as Description, t1.value as Value, t1.id as Id,
                        t2.description as t2_Description, t2.value as t2_Value, t2.id as t2_Id,
                        t3.description as t3_Description, t3.value as t3_Value, t3.id as t3_Id,
                        t4.description as t4_Description, t4.value as t4_Value, t4.id as t4_Id, t4.Date as t4_Date,
                        t5.description as t5_Description, t5.value as t5_Value, t5.id as t5_Id, t5.Date as t5_Date
                        from test t1
                        join test t2 on t1.id = t2.value / 10 - 1
                        join test t3 on t1.id = t3.id and t3.id < 2
                        join test t4 on t1.id = t4.id - 1
                        join test t5 on t1.id = t5.id - 1
                        order by t1.id",
                        col2Strategy = Prefix "t2_",
                        col3Strategy = Prefix "t3_",
                        col4Strategy = Prefix "t4_",
                        col5Strategy = Prefix "t5_",
                        typeMap = typeMap
                    ) 
                    |> List.ofSeq
                Expect.sequenceEqual 
                    res 
                    [
                        {Id = 1; Description = "this is a test 1"; Value = 10.1}, 
                        {Id = 2; Description = "this is another test"; Value = 20.0}, 
                        {Id = 1; Description = "this is a test 1"; Value = 10.1},
                        {Id = 2; Description = "this is another test"; Value = 20.0; Date = (Some (DateTime(2022, 6, 21)))},
                        {Id = 2; Description = "this is another test"; Value = 20.0}
                    ] "Records equal"
        

        ]
    ]
