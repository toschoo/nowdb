include("./now.jl")
# using NoW

NoW.withconnection("catull.schoofs.com", "50677", "foo", "bar", "wmo") do con
  s = "select a as b, count(*) as cnt, c, d , 'hello' as string, '' as empty, 123 as num, 'πυθαγορασ' as utf from mytab"
  m = match(r"from", s)
  as = NoW._parseselect(con,s)

  if size(as,1) != 8
     error("not eight fields: $(size(as))")
  end

  println(as)

  as = NoW._parseselect(con, "select * from station")
  println(as)
end
