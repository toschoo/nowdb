bin/writecsv > rsc/kilo.csv
echo "edge;origin;destin;label;timestamp;weight;weight2;wtype;wtype2" > rsc/context.csv
cat rsc/kilo.csv >> rsc/context.csv
cat test/sql/load2.sql | bin/scopetool /opt/dbs
