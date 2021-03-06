module FMapper.Tests.RelMapper

open System
open System.Data.Common
open Expecto
open FMapper

let ni<'t>() : 't = 
    raise (new NotImplementedException())
    Unchecked.defaultof<'t>

type FakeTxn(conn) =
    inherit DbTransaction()

    override self.DbConnection = conn
    override self.IsolationLevel = Data.IsolationLevel.Unspecified
    override self.Commit() = ()
    override self.Rollback() = ()

type FakeDbParameterCollection() =
    inherit DbParameterCollection()

    let pars = ResizeArray<DbParameter>()

    override self.get_Count() = pars.Count
    override self.get_SyncRoot() = null
    override self.Add(value: obj) = pars.Add(unbox value); pars.Count
    override self.AddRange(values: Array) = pars.AddRange (values |> Seq.cast |> Array.ofSeq)
    override self.Clear() = pars.Clear()
    override self.Contains(value: obj) = pars.Contains (unbox value)
    override self.Contains(value: string) = false
    override self.CopyTo(array: Array, index: int) = ()
    override self.GetEnumerator() = pars.GetEnumerator()
    override self.GetParameter(index: int) = pars.[index]
    override self.GetParameter(name : string) = pars |> Seq.find (fun x -> x.ParameterName = name)
    override self.IndexOf(value: obj) = ni<int>()
    override self.IndexOf(parameterName: string) = ni<int>()
    override self.Insert(index: int, value: obj) = ni()
    override self.Remove(value: obj) = ni()
    override self.RemoveAt(index: int) = ni<unit>()
    override self.RemoveAt(parameterName: string) = ni<unit>()
    override self.SetParameter(index: int, value: DbParameter) = ni<unit>()
    override self.SetParameter(parameterName: string, value: DbParameter) = ni<unit>()
        
and FakeDbParameter() =
    inherit DbParameter()

    let mutable name = ""
    let mutable value = null

    override self.get_DbType() = ni()
    override self.set_DbType(value: Data.DbType) = ni()
    override self.get_Direction() = ni()
    override self.set_Direction(value: Data.ParameterDirection) = ni()
    override self.get_IsNullable() = ni()
    override self.set_IsNullable(value: bool) = ni()
    override self.get_ParameterName() = name
    override self.set_ParameterName(value: string)  = name <- value
    override self.get_Size() = ni()
    override self.set_Size(value: int) = ni()
    override self.get_SourceColumn() = ni()
    override self.set_SourceColumn(value: string) = ni()
    override self.get_SourceColumnNullMapping() = ni()
    override self.set_SourceColumnNullMapping(value: bool) = ni()
    override self.get_Value() = value
    override self.set_Value(v: obj) = value <- v
    override self.ResetDbType() = ni()
    
type FakeDbCommand(?query, ?conn, ?txn) =
    inherit DbCommand()

    let mutable dr = new Common.DataReader([], [])

    let pars = FakeDbParameterCollection()

    override self.CommandText with get() = query |> Option.defaultWith (fun () -> failwithf "query not provided") and set(_) = ()
    override self.CommandTimeout with get() = 0 and set(_) = ()
    override self.CommandType with get() = Data.CommandType.Text and set(_) = ()
    override self.DbConnection with get() = conn |> Option.defaultWith (fun () -> failwithf "conn not provided") and set(_) = ()
    override self.DbTransaction with get() = txn |> Option.defaultWith (fun () -> failwithf "conn not provided") and set(_) = ()
    override self.DbParameterCollection = pars

    override self.DesignTimeVisible with get() = false and set(_) = ()
    override self.UpdatedRowSource with get() = Data.UpdateRowSource.None and set(_) = ()
    override self.Cancel() = ()
    override self.CreateDbParameter() = FakeDbParameter()
    override self.ExecuteDbDataReader(behavior: Data.CommandBehavior) = dr
    override self.ExecuteNonQuery() = 0
    override self.ExecuteScalar() = null
    override self.Prepare() = ()

    member self.SetDR(x) = dr <- x

type FakeDbConnection(schema, data) as self =
    inherit DbConnection()

    let cmd = new FakeDbCommand(null, self)
    do cmd.SetDR(new Common.DataReader(schema, data))

    override self.ConnectionString with get() = "" and set(_) = ()
    override self.Database = ""
    override self.DataSource = ""
    override self.ServerVersion = ""
    override self.State = Data.ConnectionState.Open
    override self.BeginDbTransaction(isolationLevel: Data.IsolationLevel) = new FakeTxn(self)
    override self.ChangeDatabase(databaseName: string) = ()
    override self.Close() = ()
    override self.CreateDbCommand() = cmd
    override self.Open() = ()

    member __.Cmd = cmd

type RecA = 
    {
        Id : Guid
        Description : string
    }

type RecB =
    {
        Id : int
        Value : float
    }

[<Tests>]
let testsRecord =
    testList "test RelMapper wrapper" [
        testCase "basic query" <| fun _ ->
            use conn = new FakeDbConnection([Common.SchemaItem.N("id", typeof<int>); Common.SchemaItem.N("x", typeof<int>)], [[1; 2]])
            let res = RelMapper.Query<int*int>(conn, "select * from sometable")

            Expect.sequenceEqual res [(1, 2)] "no results"

        testCase "ParameterList" <| fun _ ->
            use conn = new FakeDbConnection([Common.SchemaItem.N("id", typeof<int>); Common.SchemaItem.N("x", typeof<int>)], [])
            let res = RelMapper.Query<int*int>(conn, "select * from sometable", ParameterList.Empty |> ParameterList.add "a" 1 |> ParameterList.add "b" "test")

            Expect.isEmpty res "no results"

            let pars = [for x in conn.Cmd.Parameters do x.ParameterName, x.Value]

            Expect.sequenceEqual pars ["a", box 1; "b", box "test"] "parameters"

        testCase "parameters from anonymous record" <| fun _ ->
            use conn = new FakeDbConnection([Common.SchemaItem.N("id", typeof<int>); Common.SchemaItem.N("x", typeof<int>)], [])
            let res = RelMapper.Query<int*int, _>(conn, "select * from sometable", {| a = 1; b = "test" |})

            Expect.isEmpty res "no results"

            let pars = [for x in conn.Cmd.Parameters do x.ParameterName, x.Value]

            Expect.sequenceEqual pars ["a", box 1; "b", box "test"] "parameters"

        testCase "multimapper with tuples explicit" <| fun _ ->
            use conn = 
                new FakeDbConnection(
                    [
                        Common.SchemaItem.N("a_id", typeof<Guid>)
                        Common.SchemaItem.N("a_description", typeof<string>)
                        Common.SchemaItem.N("b_id", typeof<int>)
                        Common.SchemaItem.N("b_value", typeof<float>)
                        ], [
                            [Guid("86C17673-CDE4-4E00-99C6-49AA6F72EABE"); "first record"; 1; 10.0]
                            [Guid("86C17673-CDE4-4E00-99C6-49AA6F72EABE"); "first record"; 2; 20.0]
                            [Guid("3D5A33D2-6179-44A4-AB75-D9455605E62D"); "second record"; 1; 1.1]
                        ]
                )

            let res = RelMapper.Query<Guid*string, int*float>(conn, "select * from sometable", col2Strategy = StartIndex 2)

            Expect.sequenceEqual res [
                (Guid("86C17673-CDE4-4E00-99C6-49AA6F72EABE"), "first record"), (1, 10.)
                (Guid("86C17673-CDE4-4E00-99C6-49AA6F72EABE"), "first record"), (2, 20.)
                (Guid("3D5A33D2-6179-44A4-AB75-D9455605E62D"), "second record"), (1, 1.1)
            ] "results"

        testCase "multimapper with records" <| fun _ ->
            use conn = 
                new FakeDbConnection(
                    [
                        Common.SchemaItem.N("a_Id", typeof<Guid>)
                        Common.SchemaItem.N("a_Description", typeof<string>)
                        Common.SchemaItem.N("b_Id", typeof<int>)
                        Common.SchemaItem.N("b_Value", typeof<float>)
                        ], [
                            [Guid("86C17673-CDE4-4E00-99C6-49AA6F72EABE"); "first record"; 1; 10.0]
                            [Guid("86C17673-CDE4-4E00-99C6-49AA6F72EABE"); "first record"; 2; 20.0]
                            [Guid("3D5A33D2-6179-44A4-AB75-D9455605E62D"); "second record"; 1; 1.1]
                        ]
                )

            let res = RelMapper.Query<RecA, RecB>(conn, "select * from sometable", col1Strategy = Prefix "a_", col2Strategy = Prefix "b_")

            Expect.sequenceEqual res [
                {Id = Guid("86C17673-CDE4-4E00-99C6-49AA6F72EABE"); Description = "first record"}, {Id = 1; Value = 10.}
                {Id = Guid("86C17673-CDE4-4E00-99C6-49AA6F72EABE"); Description = "first record"}, {Id = 2; Value = 20.}
                {Id = Guid("3D5A33D2-6179-44A4-AB75-D9455605E62D"); Description = "second record"}, {Id = 1; Value = 1.1}
            ] "results"

        testCase "multimapper with mixed types" <| fun _ ->
            use conn = 
                new FakeDbConnection(
                    [
                        Common.SchemaItem.N("Id", typeof<Guid>)
                        Common.SchemaItem.N("Description", typeof<string>)
                        Common.SchemaItem.N("OtherId", typeof<int>)
                        Common.SchemaItem.N("Value", typeof<float>)
                        ], [
                            [Guid("86C17673-CDE4-4E00-99C6-49AA6F72EABE"); "first record"; 1; 10.0]
                            [Guid("86C17673-CDE4-4E00-99C6-49AA6F72EABE"); "first record"; 2; 20.0]
                            [Guid("3D5A33D2-6179-44A4-AB75-D9455605E62D"); "second record"; 1; 1.1]
                        ]
                )

            let res = RelMapper.Query<RecA, int*float>(conn, "select * from sometable", col2Strategy = StartIndex 2)

            Expect.sequenceEqual res [
                {Id = Guid("86C17673-CDE4-4E00-99C6-49AA6F72EABE"); Description = "first record"}, (1, 10.)
                {Id = Guid("86C17673-CDE4-4E00-99C6-49AA6F72EABE"); Description = "first record"}, (2, 20.)
                {Id = Guid("3D5A33D2-6179-44A4-AB75-D9455605E62D"); Description = "second record"}, (1, 1.1)
            ] "results"


    ]
