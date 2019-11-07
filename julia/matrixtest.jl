include("./now.jl")
# using NoW
using Printf: @printf

NoW.withconnection("catull.schoofs.com", "50677", "foo", "bar", "wmo", con -> begin
  m = NoW.fill(con, "select * from station", 11, count="select count(*) from station")
  sz = size(m,1)
  for i=1:sz
    @printf("%05d) %s | %d (: %s) | %s | %s | %s | %s | %f | %f\n", i,
           m[i,1], m[i,3], typeof(m[i,3]), m[i,4], m[i,5], m[i,6], m[i,7], m[i,8], m[i,9])
  end
end)
