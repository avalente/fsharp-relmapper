module FMapper.Tests.Common

open System
open System.Data.Common

type SchemaItem = 
    {
        Name : string
        Type : Type
        Nullable : bool
    }

    static member N(name, type_, ?nullable) = 
        {
            Name = name
            Type = type_
            Nullable = defaultArg nullable false
        }

type DataReader(schema : SchemaItem list, data : obj list list) =
    inherit DbDataReader( )

    let nameMap = schema |> List.mapi (fun i x -> x.Name, i) |> Map.ofList

    let mutable idx = -1

    override self.get_Depth() = -1
    override self.get_FieldCount() = schema.Length
    override self.get_HasRows() = idx < data.Length
    override self.get_IsClosed() = idx >= data.Length
    override self.get_Item(ordinal: int) = data.[idx].[ordinal]
    override self.get_Item(name: string) = data.[idx].[nameMap.[name]]
    override self.get_RecordsAffected() = data.Length
    override self.GetBytes(ordinal: int, dataOffset: int64, buffer: byte[], bufferOffset: int, length: int) = -1L
    override self.GetChars(ordinal: int, dataOffset: int64, buffer: char[], bufferOffset: int, length: int) = -1L
    override self.GetDataTypeName(ordinal: int) = failwithf "not implemented"
    override self.GetValue(ordinal: int) = self.[ordinal]
    override self.GetValues(values: obj[]) = -1
    override self.IsDBNull(ordinal: int) = self.[ordinal] |> isNull
    override self.NextResult() = false
    override self.Read() =
        if idx = data.Length - 1 then false
        else
          idx <- idx + 1
          true
    override self.GetName(ordinal: int) = schema.[ordinal].Name
    override self.GetOrdinal(name: string) = nameMap.[name]
      
    override self.GetBoolean(ordinal: int) = self.[ordinal] |> unbox<bool>
    override self.GetByte(ordinal: int) = self.[ordinal] |> unbox<byte>
    override self.GetChar(ordinal: int) = self.[ordinal] |> unbox<char>
    override self.GetDateTime(ordinal: int) = self.[ordinal] |> unbox<DateTime>
    override self.GetDecimal(ordinal: int) = self.[ordinal] |> unbox<Decimal>
    override self.GetDouble(ordinal: int) = self.[ordinal] |> unbox<Double>
    override self.GetEnumerator() = (Seq.ofList data).GetEnumerator()
    override self.GetFieldType(ordinal: int) = schema.[ordinal].Type
    override self.GetFloat(ordinal: int) = self.[ordinal] |> unbox<Single>
    override self.GetGuid(ordinal: int) = self.[ordinal] |> unbox<Guid>
    override self.GetInt16(ordinal: int) = self.[ordinal] |> unbox<int16>
    override self.GetInt32(ordinal: int) = self.[ordinal] |> unbox<int>
    override self.GetInt64(ordinal: int) = self.[ordinal] |> unbox<int64>
    override self.GetString(ordinal: int) = self.[ordinal] |> unbox<string>

    override self.GetSchemaTable() =
        let dt = new Data.DataTable()
        dt.Columns.Add("AllowDBNull", typeof<bool>) |> ignore
        dt.Columns.Add("ColumnName", typeof<string>) |> ignore
        dt.Columns.Add("ColumnOrdinal", typeof<int>) |> ignore
        dt.Columns.Add("ColumnSize", typeof<int>) |> ignore
        dt.Columns.Add("DataTypeName", typeof<string>) |> ignore
        dt.Columns.Add("NumericPrecision", typeof<int16>) |> ignore
        dt.Columns.Add("NumericScale", typeof<int16>) |> ignore

        for i, si in Seq.indexed schema do
            dt.Rows.Add([|
                box si.Nullable
                si.Name
                i
                -1
                si.Type
                -1 
                -1
            |]) |> ignore
        dt

