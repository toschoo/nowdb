include("./now.jl")
# using NoW

con = NoW.connect("catull.schoofs.com", "50677", "foo", "bar")
NoW.execute(con, "use wmo")
cur = NoW.execute(con, "select * from station")
z=0
k=0
for row in cur
    global z
    global k
    if z == 0 k=NoW.fieldcount(row) end
    z += 1
    print("$z -- ")
    for i=0:k
        v = NoW.field(row, i)
        if !isa(v, Nothing) print(" $v |") end
    end
    println("")
end
NoW.release(cur)
NoW.close(con)
