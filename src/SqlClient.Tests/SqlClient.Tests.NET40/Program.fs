﻿
open FSharp.Data

let get42 = new SqlCommand<"SELECT 42", "Server=.;Integrated Security=True">()
get42.Execute() |> Seq.toArray |> printfn "%A" 

