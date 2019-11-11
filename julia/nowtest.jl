include("./now.jl")
# using NoW

NoW.withconnection("catull.schoofs.com", "50677", "foo", "bar", "wmo") do con
  z = 0
  for row in NoW.execute(con, "select * from station") |> NoW.asarray
    z += 1
    print("$z) ")
    for v in row
        if !isa(v, Nothing) print(" $v |") end
    end
    println("")
  end
end
