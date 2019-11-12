include("./now.jl")
# using NoW
using Printf: @printf
using Profile

# NoW.withconnection("catull.schoofs.com", "50677", "foo", "bar", "wmo") do con
NoW.withconnection("localhost", "50677", "foo", "bar", "wmo") do con
  m, x = @timed NoW.fill(con, "select * from station", count="select count(*) from station")
  """
  sz = size(m,1)
  for i=1:sz
    @printf("%05d) %s | %d (: %s) | %s | %s | %s | %s | %f | %f\n", i,
           m[i,1], m[i,3], typeof(m[i,3]), m[i,4], m[i,5], m[i,6], m[i,7], m[i,8], m[i,9])
  end
  """
  return x
end
