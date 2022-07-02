module FMapperBenchmark.Main

open System

open Dapper
open Dapper.FSharp
open BenchmarkDotNet.Attributes
open FMapper
open BenchmarkDotNet.Running

let Records = 10000

module SqlServer =
    open Microsoft.Data.SqlClient

    type Record =
        {
            col1 : int16
            col2 : int
            col3 : int64
            col4 : single
            col5 : double
            col6 : decimal
            col7 : bool
            col8 : DateTime
            col9 : DateTime
            col10 : string
            col11 : Guid
            col12 : int16 option
            col13 : int option
            col14 : int64 option
            col15 : single option
            col16 : double option
            col17 : decimal option
            col18 : bool option
            col19 : DateTime option
            col20 : DateTime option
            col21 : string option
            col22 : Guid option
        }

    let createTable =
        "create table BenchmarkItem (
            col1 smallint not null,
            col2 int not null,
            col3 bigint not null,
            col4 real not null,
            col5 double precision not null,
            col6 decimal(15, 4) not null,
            col7 bit not null,
            col8 date not null,
            col9 datetime2 not null,
            col10 varchar(255) not null,
            col11 uniqueidentifier not null,
            col12 smallint null,
            col13 int null,
            col14 bigint null,
            col15 real null,
            col16 double precision null,
            col17 decimal(15, 4) null,
            col18 bit null,
            col19 date null,
            col20 datetime2 null,
            col21 varchar(255) null,
            col22 uniqueidentifier null,
        )"

    let dropTable = "drop table BenchmarkItem"

    let connString() = Environment.GetEnvironmentVariable("CONNECTION_STRING")

    type Benchmark() =
        let mutable conn : SqlConnection = null

        [<GlobalSetup>]
        member self.Setup() =
            OptionTypes.register()

            try
                self.TearDown()
            with _ -> ()
            use conn = new SqlConnection(connString())
            conn.Open()
            let txn = conn.BeginTransaction()

            printfn "Creating table..."
            use cmd = new SqlCommand(createTable, conn, txn)
            match cmd.ExecuteNonQuery() with
            | -1 -> ()
            | n -> failwithf "create table: expected -1, got %d" n

            use cmd = new SqlCommand("select * from BenchmarkItem", conn, txn)
            use adapter = new SqlDataAdapter(cmd)
            use dt = new Data.DataTable()
            adapter.Fill(dt) |> ignore
            printfn "Filling table..."
            for i = 1 to Records do
                let row = dt.NewRow()
                row.["col1"] <- i
                row.["col2"] <- i * 2
                row.["col3"] <- i * 10
                row.["col4"] <- single i
                row.["col5"] <- double i * 10.
                row.["col6"] <- decimal i / 10M
                row.["col7"] <- i % 2 = 0
                row.["col8"] <- DateTime(2022, 1, 1).AddDays(float i)
                row.["col9"] <- DateTime(2022, 1, 1).AddDays(float i).AddHours(i)
                row.["col10"] <- sprintf "row number %d" i
                row.["col11"] <- Guid.NewGuid()

                if i % 11 = 0 then
                    row.["col12"] <- DBNull.Value
                else
                    row.["col12"] <- i

                if i % 11 = 1 then
                    row.["col13"] <- DBNull.Value
                else    
                    row.["col13"] <- i * 2

                if i % 11 = 2 then
                    row.["col14"] <- DBNull.Value
                else
                    row.["col14"] <- i * 10

                if i % 11 = 3 then
                    row.["col15"] <- DBNull.Value
                else    
                    row.["col15"] <- single i

                if i % 11 = 4 then
                    row.["col16"] <- DBNull.Value
                else    
                    row.["col16"] <- double i * 10.

                if i % 11 = 5 then
                    row.["col17"] <- DBNull.Value
                else    
                    row.["col17"] <- decimal i / 10M

                if i % 11 = 6 then
                    row.["col18"] <- DBNull.Value
                else    
                    row.["col18"] <- i % 2 = 0

                if i % 11 = 7 then
                    row.["col19"] <- DBNull.Value
                else
                    row.["col19"] <- DateTime(2022, 1, 1).AddDays(float i)

                if i % 11 = 8 then
                    row.["col20"] <- DBNull.Value
                else
                    row.["col20"] <- DateTime(2022, 1, 1).AddDays(float i).AddHours(i)

                if i % 11 = 9 then
                    row.["col21"] <- DBNull.Value
                else
                    row.["col21"] <- sprintf "row number %d" i

                if i % 11 = 10 then
                    row.["col22"] <- DBNull.Value
                else
                    row.["col22"] <- Guid.NewGuid()

                dt.Rows.Add row

            let opts = SqlBulkCopyOptions.Default ||| SqlBulkCopyOptions.KeepNulls
            use bc = new SqlBulkCopy(conn, opts, txn)
            bc.DestinationTableName <- "BenchmarkItem"
            bc.NotifyAfter <- 1000
            bc.SqlRowsCopied.Add(fun x -> printfn "%d rows inserted" x.RowsCopied)
            bc.WriteToServer(dt)

            txn.Commit()

            // pre-execute the query in order to fill the cache
            using (new SqlConnection(connString())) <| fun conn ->
                conn.Open()
                use cmd = new SqlCommand("select * from BenchmarkItem", conn)
                use reader = cmd.ExecuteReader()
                while reader.Read() do reader.GetValue(0) |> ignore

        [<GlobalCleanup>]
        member self.TearDown() =
            use conn = new SqlConnection(connString())
            conn.Open()
            use cmd = new SqlCommand(dropTable, conn)
            match cmd.ExecuteNonQuery() with
            | -1 -> ()
            | n -> failwithf "drop table: expected -1, got %d" n

        [<IterationSetup>]
        member self.Connect() =
            conn <- new SqlConnection(connString())
            do conn.Open()

        [<IterationCleanup>]
        member self.CloseConnection() =
            conn.Dispose()
    
        [<Benchmark>]
        member self.RelMapper() =
            let mutable n = 0
            let res = RelMapper.Query<Record>(conn, "select * from BenchmarkItem")
            // iterate the results
            let it = res.GetEnumerator()
            while it.MoveNext() do n <- n + 1
            if n <> Records then failwithf "Wrong number of rows read: %d" n

        [<Benchmark>]
        member self.Dapper() =
            let mutable n = 0
            let res = conn.Query<Record>("select * from BenchmarkItem", buffered=false)
            // iterate the results
            let it = res.GetEnumerator()
            while it.MoveNext() do n <- n + 1
            if n <> Records then failwithf "Wrong number of rows read: %d" n

        [<Benchmark>]
        member self.Manual() =
            use cmd = new SqlCommand("select * from BenchmarkItem", conn)
            use reader = cmd.ExecuteReader()
            let mutable n = 0

            let res = 
                seq {
                    while reader.Read() do
                        {
                            col1 = reader.GetInt16(0)
                            col2 = reader.GetInt32(1)
                            col3 = reader.GetInt64(2)
                            col4 = reader.GetFloat(3)
                            col5 = reader.GetDouble(4)
                            col6 = reader.GetDecimal(5)
                            col7 = reader.GetBoolean(6)
                            col8 = reader.GetDateTime(7)
                            col9 = reader.GetDateTime(8)
                            col10 = reader.GetString(9)
                            col11 = reader.GetGuid(10)
                            col12 = if reader.IsDBNull(11) then None else reader.GetInt16(11) |> Some
                            col13 = if reader.IsDBNull(12) then None else reader.GetInt32(12) |> Some
                            col14 = if reader.IsDBNull(13) then None else reader.GetInt64(13) |> Some
                            col15 = if reader.IsDBNull(14) then None else reader.GetFloat(14) |> Some
                            col16 = if reader.IsDBNull(15) then None else reader.GetDouble(15) |> Some
                            col17 = if reader.IsDBNull(16) then None else reader.GetDecimal(16) |> Some
                            col18 = if reader.IsDBNull(17) then None else reader.GetBoolean(17) |> Some
                            col19 = if reader.IsDBNull(18) then None else reader.GetDateTime(18) |> Some
                            col20 = if reader.IsDBNull(19) then None else reader.GetDateTime(19) |> Some
                            col21 = if reader.IsDBNull(20) then None else reader.GetString(20) |> Some
                            col22 = if reader.IsDBNull(21) then None else reader.GetGuid(21) |> Some
                        }
                } 

            // iterte the results
            let it = res.GetEnumerator()
            while it.MoveNext() do n <- n + 1
            if n <> Records then failwithf "Wrong number of rows read: %d" n


[<EntryPoint>]
let main argv = 
    if argv.Length = 0 then
        printfn "Usage: benchmark.exe <connection string>"
        1
    else
        Environment.SetEnvironmentVariable("CONNECTION_STRING", argv.[0])
        let summary = BenchmarkRunner.Run<SqlServer.Benchmark>()

        0