module FSharp.RelMapper.Tests.Adapter

open System
open System.Data.Common
open Expecto
open FSharp.RelMapper
open FSharp.RelMapper.Tests.Common
        
type BasicTypes =
    {
        FInt16 : int16
        FInt : int
        FInt64 : int64
        FSingle : single
        FDouble : double
        FDecimal : decimal
        FBool : bool
        FDateTime : DateTime
        FString : string
        FGuid : Guid
    }

type BasicTypesOption =
    {
        OInt16 : int16 option
        OInt : int option
        OInt64 : int64 option
        OSingle : single option
        ODouble : double option
        ODecimal : decimal option
        OBool : bool option
        ODateTime : DateTime option
        OString : string option
        OGuid : Guid option
    }

type MyUnion =
    | First
    | Second
    | Third

[<Struct>]
type CustomType =
    {
        Id : int
        Union : MyUnion
    }
    
type CustomTypeComposite =
    {
        Id : int
        Data : Result<CustomType, string>
    }

let (=>) k v = k * v

let adapt<'t> typeMap typeAdapterMap reader =
    RelMapper.Adapt<'t>(typeMap, typeAdapterMap, reader)

[<Tests>]
let testsRecord =
    testList "test raw adapter" [
        testCase "record with basic types" <| fun _ ->
            let data = [
                [box 1s; 2; 3L; 4.0f; 5.0; 6M; false; DateTime(2022, 6, 15); "test-1"; Guid.NewGuid(); 0]
                [box 10s; 20; 30L; 40.0f; 50.0; 60M; true; DateTime(2022, 12, 31); "test 2"; Guid.NewGuid(); 0]
            ]

            let schema = 
                [
                    SchemaItem.N("FInt16", typeof<int16>)
                    SchemaItem.N("FInt", typeof<int>)
                    SchemaItem.N("FInt64", typeof<int64>)
                    SchemaItem.N("FSingle", typeof<single>)
                    SchemaItem.N("FDouble", typeof<double>)
                    SchemaItem.N("FDecimal", typeof<decimal>)
                    SchemaItem.N("FBool", typeof<bool>)
                    SchemaItem.N("FDateTime", typeof<DateTime>)
                    SchemaItem.N("FString", typeof<string>)
                    SchemaItem.N("FGuid", typeof<Guid>)
                    SchemaItem.N("ExtraField", typeof<int>)
                ]

            use reader = new DataReader(schema, data)

            let adapter = adapt<BasicTypes> CustomTypeMap.Empty CustomTypeAdapterMap.Empty reader 

            let res = [| while reader.Read() do adapter () |]

            Expect.equal res.Length 2 "Records retrieved"

            Expect.equal res.[0] {
                FInt16 = data.[0].[0] |> unbox
                FInt = data.[0].[1] |> unbox
                FInt64 = data.[0].[2] |> unbox
                FSingle = data.[0].[3] |> unbox
                FDouble = data.[0].[4] |> unbox
                FDecimal = data.[0].[5] |> unbox
                FBool = data.[0].[6] |> unbox
                FDateTime = data.[0].[7] |> unbox
                FString = data.[0].[8] |> unbox
                FGuid = data.[0].[9] |> unbox
            } "first row"

            Expect.equal res.[1] {
                FInt16 = data.[1].[0] |> unbox
                FInt = data.[1].[1] |> unbox
                FInt64 = data.[1].[2] |> unbox
                FSingle = data.[1].[3] |> unbox
                FDouble = data.[1].[4] |> unbox
                FDecimal = data.[1].[5] |> unbox
                FBool = data.[1].[6] |> unbox
                FDateTime = data.[1].[7] |> unbox
                FString = data.[1].[8] |> unbox
                FGuid = data.[1].[9] |> unbox
            } "second row"

        testCase "record with basic nullable types" <| fun _ ->
            let schema = 
                [
                    SchemaItem.N("OInt16", typeof<int16>, true)
                    SchemaItem.N("OInt", typeof<int>, true)
                    SchemaItem.N("OInt64", typeof<int64>, true)
                    SchemaItem.N("OSingle", typeof<single>, true)
                    SchemaItem.N("ODouble", typeof<double>, true)
                    SchemaItem.N("ODecimal", typeof<decimal>, true)
                    SchemaItem.N("OBool", typeof<bool>, true)
                    SchemaItem.N("ODateTime", typeof<DateTime>, true)
                    SchemaItem.N("OString", typeof<string>, true)
                    SchemaItem.N("OGuid", typeof<Guid>, true)
                ]

            let data = [
                for i = 0 to schema.Length-1 do
                    let a = [|box 1s; 2; 3L; 4.0f; 5.0; 6M; false; DateTime(2022, 6, 15); "test-1"; Guid.NewGuid()|]
                    a.[i] <- null
                    List.ofArray a
            ]

            use reader = new DataReader(schema, data)

            let adapter = adapt<BasicTypesOption> CustomTypeMap.Empty CustomTypeAdapterMap.Empty reader 

            let res = [| while reader.Read() do adapter () |]

            Expect.equal res.Length data.Length "Records retrieved"

            for i in 0..schema.Length-1 do
                Expect.equal res.[i] {
                    OInt16 = if i = 0 then None else data.[i].[0] |> unbox |> Some
                    OInt = if i = 1 then None else data.[i].[1] |> unbox |> Some
                    OInt64 = if i = 2 then None else data.[i].[2] |> unbox |> Some
                    OSingle = if i = 3 then None else data.[i].[3] |> unbox |> Some
                    ODouble = if i = 4 then None else data.[i].[4] |> unbox |> Some
                    ODecimal = if i = 5 then None else data.[i].[5] |> unbox |> Some
                    OBool = if i = 6 then None else data.[i].[6] |> unbox |> Some
                    ODateTime = if i = 7 then None else data.[i].[7] |> unbox |> Some
                    OString = if i = 8 then None else data.[i].[8] |> unbox |> Some
                    OGuid = if i = 9 then None else data.[i].[9] |> unbox |> Some
                } (sprintf "row %d" i)

        testCase "record with missing column (mispelled)" <| fun _ ->
            let data = []

            let schema = 
                [
                    SchemaItem.N("FInT16", typeof<int16>)
                    SchemaItem.N("FInt", typeof<int>)
                    SchemaItem.N("FInt64", typeof<int64>)
                    SchemaItem.N("FSingle", typeof<single>)
                    SchemaItem.N("FDouble", typeof<double>)
                    SchemaItem.N("FDecimal", typeof<decimal>)
                    SchemaItem.N("FBool", typeof<bool>)
                    SchemaItem.N("FDateTime", typeof<DateTime>)
                    SchemaItem.N("FString", typeof<string>)
                    SchemaItem.N("FGuid", typeof<Guid>)
                ]

            use reader = new DataReader(schema, data)

            let message = 
                Expect.throwsC 
                    (fun () -> adapt<BasicTypes> CustomTypeMap.Empty CustomTypeAdapterMap.Empty reader |> ignore)
                    (fun e -> e.Message)

            Expect.stringContains message "FInt16" "wrong exception message "

        testCase "record with missing column (missing)" <| fun _ ->
            let data = []

            let schema = 
                [
                    SchemaItem.N("FInt16", typeof<int16>)
                    SchemaItem.N("FInt", typeof<int>)
                    SchemaItem.N("FInt64", typeof<int64>)
                    SchemaItem.N("FSingle", typeof<single>)
                    SchemaItem.N("FDecimal", typeof<decimal>)
                    SchemaItem.N("FBool", typeof<bool>)
                    SchemaItem.N("FDateTime", typeof<DateTime>)
                    SchemaItem.N("FString", typeof<string>)
                    SchemaItem.N("FGuid", typeof<Guid>)
                ]

            use reader = new DataReader(schema, data)

            let message = 
                Expect.throwsC 
                    (fun () -> adapt<BasicTypes> CustomTypeMap.Empty CustomTypeAdapterMap.Empty reader |> ignore)
                    (fun e -> e.Message)

            Expect.stringContains message "FDouble" "wrong exception message "

        testCase "unsupported type" <| fun _ ->
            let data = []

            let schema = 
                [
                    SchemaItem.N("FInt16", typeof<int16>)
                    SchemaItem.N("FUInt", typeof<uint32>)
                ]

            use reader = new DataReader(schema, data)

            let message = 
                Expect.throwsC 
                    (fun () -> adapt<{| FInt16 : int16; FUInt : uint32 |}> CustomTypeMap.Empty CustomTypeAdapterMap.Empty reader |> ignore)
                    (fun e -> e.Message)

            Expect.stringContains message "Unsupported data type" "wrong exception message"
            Expect.stringContains message "UInt32" "wrong exception message"

        testCase "type mismatch" <| fun _ ->
            let data = []

            let schema = 
                [
                    SchemaItem.N("FInt16", typeof<int16>)
                ]

            use reader = new DataReader(schema, data)

            let message = 
                Expect.throwsC 
                    (fun () -> adapt<{| FInt16 : int |}> CustomTypeMap.Empty CustomTypeAdapterMap.Empty reader |> ignore)
                    (fun e -> e.Message)

            Expect.stringContains message "Type mismatch" "wrong exception message"
            Expect.stringContains message "FInt16" "wrong exception message"
            Expect.stringContains message "expected 'Int32', found 'Int16'" "wrong exception message"

        testCase "record with custom type" <| fun _ ->
            let tm = 
                CustomTypeMap.Empty
                |> CustomTypeMap.add (fun wrapper i ->
                    match wrapper.Reader.GetString(i) with
                    | "first" -> First
                    | "second" -> Second
                    | "third" -> Third
                    | x -> failwithf "Invalid value for <MyUnion>: '%s'" x
                )

            let schema = 
                [
                    SchemaItem.N("Id", typeof<int>)
                    SchemaItem.N("Union", typeof<string>)
                ]

            use reader = new DataReader(schema, [[box 1; "first"]; [box 2; "second"]])

            let adapter = adapt<CustomType> tm CustomTypeAdapterMap.Empty reader 

            let res = [| while reader.Read() do adapter () |]

            Expect.equal res.Length 2 "Records retrieved"

            Expect.equal res.[0] {
                Id = 1
                Union = First
            } "first row"

            Expect.equal res.[1] {
                Id = 2
                Union = Second
            } "second row"

        testCase "record with custom type composite" <| fun _ ->
            let tm = 
                CustomTypeMap.Empty
                |> CustomTypeMap.add (fun (wrapper : DataReaderWrapper) i ->
                    match wrapper.Reader.GetString(i) with
                    | "first" -> First
                    | "second" -> Second
                    | "third" -> Third
                    | x -> failwithf "Invalid value for <MyUnion>: '%s'" x
                )

            let ctm =
                CustomTypeAdapterMap.Empty
                |> CustomTypeAdapterMap.add "Data" (fun wrapper ->
                    match wrapper.GetInt32("Id"), wrapper.GetStringOption("value"), wrapper.GetStringOption("error") with
                    | _, None, None 
                    | _, Some _, Some _ -> failwithf "unexpected combination"
                    | Id, Some x, None -> Ok {Id = Id; Union = wrapper.ValueGetter("value", typeof<MyUnion>) () |> unbox<_>}
                    | _, None, Some x -> Error x
                )

            let schema = 
                [
                    SchemaItem.N("Id", typeof<int>)
                    SchemaItem.N("value", typeof<string>, true)
                    SchemaItem.N("error", typeof<string>, true)
                ]

            use reader = new DataReader(schema, [[box 1; "first"; null; null]; [box 2; null; "this is an error message"]])

            let adapter = adapt<CustomTypeComposite> tm ctm reader 

            let res = [| while reader.Read() do adapter () |]

            Expect.equal res.Length 2 "Records retrieved"

            Expect.equal res.[0] {
                Id = 1
                Data = Ok {Id = 1; Union = First}
            } "first row"

            Expect.equal res.[1] {
                Id = 2
                Data = Error "this is an error message"
            } "second row"

        testCase "anonymous record" <| fun _ ->
            let data = [
                [box 1s; 2; 3L; 4.0f; 5.0; 6M; false; DateTime(2022, 6, 15); "test-1"; Guid.NewGuid()]
                [box 10s; 20; 30L; 40.0f; 50.0; 60M; true; DateTime(2022, 12, 31); "test 2"; Guid.NewGuid()]
            ]

            let schema = 
                [
                    SchemaItem.N("FInt16", typeof<int16>)
                    SchemaItem.N("FInt", typeof<int>)
                    SchemaItem.N("FInt64", typeof<int64>)
                    SchemaItem.N("FSingle", typeof<single>)
                    SchemaItem.N("FDouble", typeof<double>)
                    SchemaItem.N("FDecimal", typeof<decimal>)
                    SchemaItem.N("FBool", typeof<bool>)
                    SchemaItem.N("FDateTime", typeof<DateTime>)
                    SchemaItem.N("FString", typeof<string>)
                    SchemaItem.N("FGuid", typeof<Guid>)
                ]

            use reader = new DataReader(schema, data)

            let adapter = 
                adapt<{|FInt16 : int16; FInt : int; FInt64 : int64; FSingle : single; FDouble : double; FDecimal : decimal; FBool : bool; FDateTime : DateTime; FString : string; FGuid : Guid|}> 
                    CustomTypeMap.Empty CustomTypeAdapterMap.Empty reader 

            let res = [| while reader.Read() do adapter () |]

            Expect.equal res.Length 2 "Records retrieved"

            Expect.equal res.[0] {|
                FInt16 = data.[0].[0] |> unbox
                FInt = data.[0].[1] |> unbox
                FInt64 = data.[0].[2] |> unbox
                FSingle = data.[0].[3] |> unbox
                FDouble = data.[0].[4] |> unbox
                FDecimal = data.[0].[5] |> unbox
                FBool = data.[0].[6] |> unbox
                FDateTime = data.[0].[7] |> unbox
                FString = data.[0].[8] |> unbox
                FGuid = data.[0].[9] |> unbox
            |} "first row"

            Expect.equal res.[1] {|
                FInt16 = data.[1].[0] |> unbox
                FInt = data.[1].[1] |> unbox
                FInt64 = data.[1].[2] |> unbox
                FSingle = data.[1].[3] |> unbox
                FDouble = data.[1].[4] |> unbox
                FDecimal = data.[1].[5] |> unbox
                FBool = data.[1].[6] |> unbox
                FDateTime = data.[1].[7] |> unbox
                FString = data.[1].[8] |> unbox
                FGuid = data.[1].[9] |> unbox
            |} "second row"

        testCase "tuple with basic types" <| fun _ ->
            let data = [
                [box 2; 6M; DateTime(2022, 6, 15); "test-1"]
                [box 20; (None : decimal option); DateTime(2022, 12, 31); (None : string option)]
            ]

            let schema = 
                [
                    SchemaItem.N("FInt", typeof<int>)
                    SchemaItem.N("FDecimal", typeof<decimal>, true)
                    SchemaItem.N("FDateTime", typeof<DateTime>)
                    SchemaItem.N("FString", typeof<string>, true)
                ]

            use reader = new DataReader(schema, data)

            let adapter = adapt<int * decimal option * DateTime * string option> CustomTypeMap.Empty CustomTypeAdapterMap.Empty reader 

            let res = [| while reader.Read() do adapter () |]

            Expect.equal res.Length 2 "Records retrieved"

            Expect.equal res.[0] (2, Some 6M, DateTime(2022, 6, 15), Some "test-1") "first row"
            Expect.equal res.[1] (20, None, DateTime(2022, 12, 31), None) "second row"

        testCase "tuple with custom types" <| fun _ ->
            let data = [
                [box 2; 6M; "test-1"; DateTime(2022, 6, 15)]
                [box 20; (None : decimal option); (None : string option); DateTime(2022, 12, 31)]
            ]

            let schema = 
                [
                    SchemaItem.N("FInt", typeof<int>)
                    SchemaItem.N("FDecimal", typeof<decimal>, true)
                    SchemaItem.N("FString", typeof<string>, true)
                    SchemaItem.N("FDateTime", typeof<DateTime>)
                ]

            use reader = new DataReader(schema, data)

            let adapter = 
                adapt<uint32 * {|Value : double option; Date : DateTime|} * string option> 
                    (CustomTypeMap.Empty |> CustomTypeMap.add (fun wrapper i ->
                        let value = wrapper.Reader.GetInt32(i)
                        Convert.ToUInt32 value
                    ))
                    (CustomTypeAdapterMap.Empty |> CustomTypeAdapterMap.add "1" (fun wrapper ->
                        let value = wrapper.GetDecimalOption("FDecimal") |> Option.map Convert.ToDouble
                        let date = wrapper.GetDateTime("FDateTime")
                        {|Date = date; Value = value|}
                    ))
                    reader 

            let res = [| while reader.Read() do adapter () |]

            Expect.equal res.Length 2 "Records retrieved"

            Expect.equal res.[0] (2u, {| Value = Some 6.; Date = DateTime(2022, 6, 15)|}, Some "test-1") "first row"
            Expect.equal res.[1] (20u, {| Date = DateTime(2022, 12, 31); Value = None |}, None) "second row"

        testCase "tuple with insufficient columns" <| fun _ ->
            let data = []

            let schema = 
                [
                    SchemaItem.N("FInt", typeof<int>)
                ]

            use reader = new DataReader(schema, data)

            let message = 
                Expect.throwsC  
                    (fun () -> adapt<int * int> CustomTypeMap.Empty CustomTypeAdapterMap.Empty reader |> ignore)
                    (fun e -> e.Message)

            Expect.stringContains message "only 1 columns" "wrong error message"

        testCase "adapter without type" <| fun _ ->
            use reader = new DataReader([], [])

            let message = 
                Expect.throwsC 
                    (fun () -> adapt CustomTypeMap.Empty CustomTypeAdapterMap.Empty reader |> ignore)
                    (fun e -> e.Message)

            Expect.stringContains message "maybe" "wrong message"

    ]

