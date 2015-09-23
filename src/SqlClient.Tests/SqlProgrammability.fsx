#r "../../bin/Fsharp.Data.SqlClient.dll"
//#r "Microsoft.SqlServer.Types.dll"
//#load "ConnectionStrings.fs"
open System
open System.Data
open FSharp.Data

//[<Literal>] 
//let connectionString = ConnectionStrings.AdventureWorksLiteral
////let connectionString = ConnectionStrings.AdventureWorksAzure
//
//[<Literal>] 
//let prodConnectionString = ConnectionStrings.MasterDb

type AdventureWorks = SqlClient<"Data Source=.;Initial Catalog=AdventureWorks2014;Integrated Security=True">
type dbo = AdventureWorks.dbo

let bag = AdventureWorks.CreateCommand<"Name,Grand Slams">()
type Bag = AdventureWorks.Commands.``CreateCommand,CommandText"Name,Grand Slams"``


(bag :> ISqlCommand).Execute(Array.empty)

//let get42 = AdventureWorks.CreateCommand