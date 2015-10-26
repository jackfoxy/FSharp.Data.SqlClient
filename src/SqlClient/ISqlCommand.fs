﻿namespace FSharp.Data

open System
open System.Data
open System.Data.SqlClient
open System.Reflection

open FSharp.Data.SqlClient

[<CompilerMessageAttribute("This API supports the FSharp.Data.SqlClient infrastructure and is not intended to be used directly from your code.", 101, IsHidden = true)>]
type ISqlCommand = 
    
    abstract Execute: parameters: (string * obj)[] -> obj
    abstract AsyncExecute: parameters: (string * obj)[] -> obj
    abstract ExecuteSingle: parameters: (string * obj)[] -> obj
    abstract AsyncExecuteSingle: parameters: (string * obj)[] -> obj

    abstract ToTraceString: parameters: (string * obj)[] -> string

    abstract Raw: SqlCommand with get

module Seq = 

    let internal toOption source =  
        match source |> Seq.truncate 2 |> Seq.toArray with
        | [||] -> None
        | [| x |] -> Some x
        | _ -> invalidArg "source" "The input sequence contains more than one element."

    let internal ofReader<'TItem> rowMapping (reader : SqlDataReader) = 
        seq {
            use _ = reader
            while reader.Read() do
                let values = Array.zeroCreate reader.FieldCount
                reader.GetValues(values) |> ignore
                yield values |> rowMapping |> unbox<'TItem>
        }

[<CompilerMessageAttribute("This API supports the FSharp.Data.SqlClient infrastructure and is not intended to be used directly from your code.", 101, IsHidden = true)>]
[<RequireQualifiedAccess>]
type ResultRank = 
    | Sequence = 0
    | SingleRow = 1
    | ScalarValue = 2

[<CompilerMessageAttribute("This API supports the FSharp.Data.SqlClient infrastructure and is not intended to be used directly from your code.", 101, IsHidden = true)>]
type RowMapping = obj[] -> obj


[<CompilerMessageAttribute("This API supports the FSharp.Data.SqlClient infrastructure and is not intended to be used directly from your code.", 101, IsHidden = true)>]
type DesignTimeConfig = {
    ConnectionString: ConnectionString
    SqlStatement: string
    IsStoredProcedure: bool 
    Parameters: SqlParameter[]
    ResultType: ResultType
    Rank: ResultRank
    RowMapping: RowMapping
    ItemTypeName: string
}

[<CompilerMessageAttribute("This API supports the FSharp.Data.SqlClient infrastructure and is not intended to be used directly from your code.", 101, IsHidden = true)>]
type ``ISqlCommand Implementation``(cfg: DesignTimeConfig, connection, transaction, commandTimeout) = 

    let cmd = new SqlCommand(cfg.SqlStatement, Transaction = transaction, CommandTimeout = commandTimeout)
    let connection, manageConnection = 
        if transaction <> null then transaction.Connection, false
        else 
            match connection with
            | Choice1Of2 connectionString -> new SqlConnection( connectionString), true
            | Choice2Of2 null -> new SqlConnection(cfg.ConnectionString.Value), true
            | Choice2Of2 instance -> instance, false

    do
        cmd.Connection <- connection
        cmd.CommandType <- if cfg.IsStoredProcedure then CommandType.StoredProcedure else CommandType.Text
        cmd.Parameters.AddRange( cfg.Parameters)

    let getReaderBehavior() = 
        seq {
            yield CommandBehavior.SingleResult

            if cmd.Connection.State <> ConnectionState.Open && manageConnection
            then
                cmd.Connection.Open() 
                yield CommandBehavior.CloseConnection

            if cfg.Rank = ResultRank.SingleRow then yield CommandBehavior.SingleRow 
        }
        |> Seq.reduce (|||) 

    let notImplemented _ : _ = raise <| NotImplementedException()

    let execute, asyncExecute, executeSingle, asyncExecuteSingle = 
        match cfg.ResultType with
        | ResultType.DataReader -> 
            ``ISqlCommand Implementation``.ExecuteReader >> box, 
            ``ISqlCommand Implementation``.AsyncExecuteReader >> box,
            notImplemented, 
            notImplemented
        | ResultType.DataTable ->
            ``ISqlCommand Implementation``.ExecuteDataTable >> box, 
            ``ISqlCommand Implementation``.AsyncExecuteDataTable >> box,
            notImplemented,
            notImplemented
        | ResultType.Records | ResultType.Tuples ->
            match box cfg.RowMapping, cfg.ItemTypeName with
            | null, itemTypeName when Type.GetType(itemTypeName, throwOnError = true) = typeof<Void> ->
                ``ISqlCommand Implementation``.ExecuteNonQuery manageConnection >> box, 
                ``ISqlCommand Implementation``.AsyncExecuteNonQuery manageConnection >> box,
                notImplemented, 
                notImplemented
            | rowMapping, itemTypeName ->
                assert (rowMapping <> null && itemTypeName <> null)
                let itemType = Type.GetType( itemTypeName, throwOnError = true)
                
                let executeHandle = 
                    typeof<``ISqlCommand Implementation``>
                        .GetMethod("ExecuteSeq", BindingFlags.NonPublic ||| BindingFlags.Static)
                        .MakeGenericMethod(itemType)
                
                let asyncExecuteHandle = 
                    typeof<``ISqlCommand Implementation``>
                        .GetMethod("AsyncExecuteSeq", BindingFlags.NonPublic ||| BindingFlags.Static)
                        .MakeGenericMethod(itemType)
                        
                executeHandle.Invoke(null, [| cfg.Rank; cfg.RowMapping |]) |> unbox >> box, 
                asyncExecuteHandle.Invoke(null, [| cfg.Rank; cfg.RowMapping |]) |> unbox >> box,
                executeHandle.Invoke(null, [| ResultRank.SingleRow; cfg.RowMapping |]) |> unbox >> box, 
                asyncExecuteHandle.Invoke(null, [| ResultRank.SingleRow; cfg.RowMapping |]) |> unbox >> box

        | unexpected -> failwithf "Unexpected ResultType value: %O" unexpected

    new(cfg, connectionString) = new ``ISqlCommand Implementation``(cfg, Choice1Of2 connectionString, null, SqlCommand.DefaultTimeout)

    member this.CommandTimeout = cmd.CommandTimeout

    interface ISqlCommand with

        member this.Execute parameters = execute(cmd, getReaderBehavior, parameters)
        member this.AsyncExecute parameters = asyncExecute(cmd, getReaderBehavior, parameters)
        member this.ExecuteSingle parameters = executeSingle(cmd, getReaderBehavior, parameters)
        member this.AsyncExecuteSingle parameters = asyncExecuteSingle(cmd, getReaderBehavior, parameters)

        member this.ToTraceString parameters =  
            ``ISqlCommand Implementation``.SetParameters(cmd, parameters)
            let parameterDefinition (p : SqlParameter) =
                if p.Size <> 0 then
                    sprintf "%s %A(%d)" p.ParameterName p.SqlDbType p.Size
                else
                    sprintf "%s %A" p.ParameterName p.SqlDbType 
            seq {
                
                yield sprintf "exec sp_executesql N'%s'" (cmd.CommandText.Replace("'", "''"))
              
                if cmd.Parameters.Count > 0
                then 
                    yield cmd.Parameters
                        |> Seq.cast<SqlParameter> 
                        |> Seq.map parameterDefinition
                        |> String.concat ","
                        |> sprintf "N'%s'" 

                if parameters.Length > 0 
                then 
                    yield parameters
                        |> Seq.map(fun (name,value) -> sprintf "%s='%O'" name value) 
                        |> String.concat ","
            } |> String.concat "," //Using string.concat to handle annoying case with no parameters

        member this.Raw = cmd
            
    interface IDisposable with
        member this.Dispose() =
            cmd.Dispose()

    static member internal SetParameters(cmd: SqlCommand, parameters: (string * obj)[]) = 
        for name, value in parameters do
            
            let p = cmd.Parameters.[name]            

            if value = null 
            then 
                p.Value <- DBNull.Value 
            else
                if not( p.SqlDbType = SqlDbType.Structured)
                then 
                    p.Value <- value
                else
                    let table : DataTable = unbox p.Value
                    table.Rows.Clear()
                    for rowValues in unbox<seq<obj>> value do
                        table.Rows.Add( rowValues :?> obj[]) |> ignore

            if Convert.IsDBNull p.Value 
            then 
                match p.SqlDbType with
                | SqlDbType.NVarChar -> p.Size <- 4000
                | SqlDbType.VarChar -> p.Size <- 8000
                | _ -> ()

//Execute/AsyncExecute versions

    static member internal ExecuteReader(cmd, getReaderBehavior, parameters) = 
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)
        cmd.ExecuteReader( getReaderBehavior())

    static member internal AsyncExecuteReader(cmd, getReaderBehavior, parameters) = 
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)
        cmd.AsyncExecuteReader( getReaderBehavior())
    
    static member internal ExecuteDataTable(cmd, getReaderBehavior, parameters) = 
        use cursor = ``ISqlCommand Implementation``.ExecuteReader(cmd, getReaderBehavior, parameters) 
        let result = new FSharp.Data.DataTable<DataRow>(null, cmd)
        result.Load( cursor)

        let hasOutputParameters = cmd.Parameters |> Seq.cast<SqlParameter> |> Seq.exists (fun x -> x.Direction.HasFlag( ParameterDirection.Output))

        if hasOutputParameters
        then
            for i = 0 to parameters.Length - 1 do
                let name, _ = parameters.[i]
                let p = cmd.Parameters.[name]
                if p.Direction = ParameterDirection.Output
                then 
                    parameters.[i] <- name, p.Value
        result

    static member internal AsyncExecuteDataTable(cmd, getReaderBehavior, parameters) = 
        async {
            use! reader = ``ISqlCommand Implementation``.AsyncExecuteReader(cmd, getReaderBehavior, parameters) 
            let result = new FSharp.Data.DataTable<DataRow>(null, cmd)
            result.Load(reader)
            return result
        }

    static member internal ExecuteSeq<'TItem> (rank, rowMapper) = fun(cmd, getReaderBehavior, parameters) -> 
        let xs = ``ISqlCommand Implementation``.ExecuteReader(cmd, getReaderBehavior, parameters) |> Seq.ofReader<'TItem> rowMapper

        if rank = ResultRank.SingleRow 
        then 
            xs |> Seq.toOption |> box
        elif rank = ResultRank.ScalarValue 
        then 
            xs |> Seq.exactlyOne |> box
        else 
            assert (rank = ResultRank.Sequence)
            box xs 
            
    static member internal AsyncExecuteSeq<'TItem> (rank, rowMapper) = fun(cmd, getReaderBehavior, parameters) ->
        let xs = 
            async {
                let! reader = ``ISqlCommand Implementation``.AsyncExecuteReader(cmd, getReaderBehavior, parameters)
                return reader |> Seq.ofReader<'TItem> rowMapper
            }

        if rank = ResultRank.SingleRow
        then
            async {
                let! xs = xs 
                return xs |> Seq.toOption
            }
            |> box
        elif rank = ResultRank.ScalarValue 
        then 
            async {
                let! xs = xs 
                return xs |> Seq.exactlyOne
            }
            |> box       
        else 
            assert (rank = ResultRank.Sequence)
            box xs 

    static member internal ExecuteNonQuery manageConnection (cmd, _, parameters) = 
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)  
        use openedConnection = cmd.Connection.UseLocally(manageConnection )
        let recordsAffected = cmd.ExecuteNonQuery() 
        for i = 0 to parameters.Length - 1 do
            let name, _ = parameters.[i]
            let p = cmd.Parameters.[name]
            if p.Direction = ParameterDirection.Output
            then 
                parameters.[i] <- name, p.Value
        recordsAffected

    static member internal AsyncExecuteNonQuery manageConnection (cmd, _, parameters) = 
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)  
        async {         
            use _ = cmd.Connection.UseLocally(manageConnection )
            return! cmd.AsyncExecuteNonQuery() 
        }



