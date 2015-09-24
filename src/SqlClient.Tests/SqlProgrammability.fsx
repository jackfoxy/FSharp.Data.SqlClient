#r @"C:\Program Files (x86)\Microsoft SQL Server\110\SDK\Assemblies\Microsoft.SqlServer.Types.dll"
#r "../../bin/Fsharp.Data.SqlClient.dll"
//#load "ConnectionStrings.fs"
open System
open System.Data
open FSharp.Data
open System.Data.SqlClient

[<Literal>] 
//let connectionString = ConnectionStrings.AdventureWorksLiteral
//let connectionString = ConnectionStrings.AdventureWorksAzure
let connectionString = "Data Source=.;Initial Catalog=AdventureWorks2014;Integrated Security=True"

type DB = SqlClient<connectionString>
type dbo = DB.dbo

let connection = new SqlConnection(connectionString)
connection.Open()

let getTopSalespeople = 
    DB.CreateCommand<"
        SELECT TOP(@topN) FirstName, LastName, SalesYTD 
        FROM Sales.vSalesPerson
        WHERE CountryRegionName = @regionName AND SalesYTD > @salesMoreThan 
        ORDER BY SalesYTD
    ">(commandTimeout = 60)

getTopSalespeople.Execute(topN = 3L, regionName = "United States", salesMoreThan = 1000000M) |> printfn "%A"

let get42AndTime = DB.CreateCommand<"SELECT 42, GETDATE()", ResultType.Tuples, SingleRow = true>(connection)
get42AndTime.AsyncExecute() |> Async.RunSynchronously |> printfn "%A"

let myPerson = DB.CreateCommand<"exec Person.myProc @x", ResultType.Tuples, SingleRow = true, TypeName = "MyProc">()
type MyTableType = DB.Person.``User-Defined Table Types``.MyTableType
myPerson.Execute [ MyTableType(myId = 1, myName = Some "monkey"); MyTableType(myId = 2, myName = Some "donkey") ] 

open Microsoft.SqlServer.Types
open System.Data.SqlTypes

let getEmployeeByLevel = DB.CreateCommand<"
    SELECT OrganizationNode 
    FROM HumanResources.Employee 
    WHERE OrganizationNode = @OrganizationNode", SingleRow = true>()

let p = SqlHierarchyId.Parse(SqlString("/1/1/"))

getEmployeeByLevel.Execute( p)|> printfn "%A"