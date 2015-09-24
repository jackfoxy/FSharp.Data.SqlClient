module FSharp.Data.CreateCommandTest

type DB = SqlClient<"name = AdventureWorks">

open Xunit

[<Fact>]
let getSingleRowNoParams() = 
    use cmd = DB.CreateCommand<"SELECT 42", SingleRow = true>()
    Assert.Equal(Some 42, cmd.Execute())    

[<Fact>]
let getSequenceWithParams() = 
    use cmd = 
        DB.CreateCommand<"
            SELECT TOP(@topN) FirstName, LastName, SalesYTD 
            FROM Sales.vSalesPerson
            WHERE CountryRegionName = @regionName AND SalesYTD > @salesMoreThan 
            ORDER BY SalesYTD
        ">(commandTimeout = 60)

    Assert.Equal(60, cmd.CommandTimeout)

    let xs = [ for x in cmd.Execute(topN = 3L, regionName = "United States", salesMoreThan = 1000000M) -> x.FirstName, x.LastName, x.SalesYTD ]

    let expected = [
        ("Pamela", "Ansman-Wolfe", 1352577.1325M)
        ("David", "Campbell", 1573012.9383M)
        ("Tete", "Mensa-Annan", 1576562.1966M)
    ]

    Assert.Equal<_ list>(expected, xs)

type MyTableType = DB.Person.``User-Defined Table Types``.MyTableType
[<Fact>]
let udttAndTuplesOutput() = 
    let cmd = DB.CreateCommand<"exec Person.myProc @x", ResultType.Tuples, SingleRow = true>()
    let p = [
        MyTableType(myId = 1, myName = Some "monkey")
        MyTableType(myId = 2, myName = Some "donkey")
    ]
    Assert.Equal(Some(1, Some "monkey"), cmd.Execute(x = p))    

open Microsoft.SqlServer.Types
open System.Data.SqlTypes

[<Fact>]
let spatialTypes() = 
    use cmd = 
        DB.CreateCommand<"
            SELECT OrganizationNode 
            FROM HumanResources.Employee 
            WHERE OrganizationNode = @OrganizationNode
        ", SingleRow = true>()

    let p = SqlHierarchyId.Parse(SqlString("/1/1/"))
    let result = cmd.Execute( p)
    Assert.Equal(Some(Some p), result)

[<Fact>]
let optionalParams() = 
    use cmd = DB.CreateCommand<"SELECT CAST(@x AS INT) + ISNULL(CAST(@y AS INT), 1)", SingleRow = true, AllParametersOptional = true>()
    Assert.Equal( Some( Some 14), cmd.Execute(Some 3, Some 11))    
    Assert.Equal( Some( Some 12), cmd.Execute(x = Some 11))    

[<Literal>]
let evenNumbers = "select value, svalue = cast(value as char(1)) from (values (2), (4), (6), (8)) as T(value)"

[<Fact>]
let datatableAndDataReader() = 
    use getDataReader = DB.CreateCommand<evenNumbers, ResultType.DataReader>()
    let xs = [
        use cursor = getDataReader.Execute()
        while cursor.Read() do
            yield cursor.GetInt32( 0), cursor.GetString( 1)
    ]

    let table = DB.CreateCommand<evenNumbers, ResultType.DataTable>().Execute()
    let ys = [ for row in table.Rows -> row.value, row.svalue.Value ]

    Assert.Equal<_ list>(xs, ys)