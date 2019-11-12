include("./now.jl")
# using NoW
using DataFrames
using Printf: @printf

# NoW.withconnection("catull.schoofs.com", "50677", "foo", "bar", "wmo") do con
NoW.withconnection("localhost", "50677", "foo", "bar", "wmo") do con
  df, x = @timed NoW.loadsql(con, "select * from station", count="select count(*) from station")
  println(typeof(df))
  println(DataFrames.describe(df))
  # println(df)
  #sz = size(df,1)
  #for i=1:sz
  #  @printf("%05d) %s | %d (: %s) | %s | %s | %s | %s | %f | %f\n", i,
  #          df[i,1], df[i,3], typeof(df[i,3]), df[i,4], df[i,5], df[i,6], df[i,7], df[i,8], df[i,9])
  #end
  return x
end
