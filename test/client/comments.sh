cat test/sql/create2.sql | bin/nowclient

if [ $? -ne 0 ]
then
    echo "FAILED: cannot run script"
    exit 1
fi

x=$(bin/nowclient -q -d retail -Q "show types")

p="client"
for a in $x
do
  if [ "$a" != "$p" ]
  then
    echo "FAILED: expecting '$p',  got: $a"
    exit 1
  fi
  p="product"
done

x=$(bin/nowclient -q -d retail -Q "show edges")

if [ "$x" != "buys" ]
  then
    echo "FAILED: expecting 'buys',  got: $x"
    exit 1
fi

x=$(bin/nowclient -q -d retail -Q "select count(*) from client")

if [ "$x" != "1" ]
then
    echo "FAILED: expecting clients: 1, got: $x"
    exit 1
fi

x=$(bin/nowclient -q -d retail -Q "select count(*) from product")

if [ "$x" != "1" ]
then
    echo "FAILED: expecting products: 1, got: $x"
    exit 1
fi

x=$(bin/nowclient -q -d retail -Q "select count(*) from buys")

if [ "$x" != "1" ]
then
    echo "FAILED: expecting buys: 1, got: $x"
    exit 1
fi

echo "PASSED"
