
//#r "../bin/Debug/SqlCommandProvider.dll"
#r "System.Data.DataSetExtensions"

open System.Data
open System.Data.SqlClient
//open FSharp.Data.SqlClient.Extensions
let conn = new SqlConnection("""Data Source=(LocalDb)\v11.0;Initial Catalog=AdventureWorks2012;Integrated Security=True""")
conn.Close()
conn.Open()
//printfn "%A" <| conn.GetDataTypesMapping()

//let cmd = new SqlCommand("uspSearchCandidateResumes", conn, CommandType = CommandType.StoredProcedure)
let cmd = new SqlCommand("dbo.[Init]", conn, CommandType = CommandType.StoredProcedure)
SqlCommandBuilder.DeriveParameters cmd
let reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly)


for p in cmd.Parameters do printfn "Param: %s, type: %s, sqldbtype: %A, direction %A, IsNullable %b, Value: %A" p.ParameterName p.TypeName p.SqlDbType p.Direction p.IsNullable p.Value

conn.GetSchema("DataTypes").AsEnumerable() 
|> Seq.map (fun r -> r.["TypeName"], r.["ProviderDbType"], r.["DataType"], r.["IsBestMatch"])
|> Array.ofSeq 

let cmd = new SqlCommand("SELECT CAST(10 AS TINYINT) AS Value", conn)
conn.Open()
let xs = cmd.ExecuteReader(CommandBehavior.CloseConnection ||| CommandBehavior.SingleRow)
let values = Array.zeroCreate<obj> xs.FieldCount
xs.Read()
let c = xs.GetValues values 
assert(c = values.Length)
printfn "values: %A" values
printfn "type: %s" <| values.[0].GetType().Name
printfn "value: %i" <| (unbox<sbyte> values.[0])

let cmdClone = cmd.Clone()
cmd.CommandText = cmdClone.CommandText
cmdClone.CommandText <- "SELECT 0 AS X"
//cmdClone.Connection <- cmd.Connection.Clone()
cmd.Connection = cmdClone.Connection
//
open System.Dynamic
open System.Collections.Generic
let (?) (this : ExpandoObject) prop =
    match (this :> IDictionary<_, _>).[prop] with
    | null -> None
    | value -> Some(unbox value)

let expando = ExpandoObject()
let dict : System.Collections.Generic.IDictionary<string, obj> = upcast expando
dict.["test"] <- 15
let x : int option = expando ? test