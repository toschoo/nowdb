if [ $# -lt 1 ]
then
	ibase=/home/pj/pknoa/rsc/jm
else 
	ibase=$1
fi
if [ $# -lt 2 ]
then
	obase=/opt/dbs
else
	obase=$2
fi
if [ $# -lt 3 ]
then
	lines=0
else
	lines=$3
fi

if [ $lines -eq 0 ]
then
	unzip -p $ibase/pdoptproducts2016093000000001.dat.zip > \
	         $ibase/products.$lines.dat
else
	unzip -p $ibase/pdoptproducts2016093000000001.dat.zip | \
	       head -$lines > $ibase/products.$lines.dat
fi

if [ $lines -eq 0 ]
then
	unzip -p $ibase/pdoptcards2016093000000001.dat.zip  | \
		       grep -v "Baguim do Monte |" | \
		       grep -v "419826900323"      | \
		       grep -v "419820080124"      | \
		       grep -v "419821570093"      | \
		       grep -v "419819700409"      > \
	         $ibase/clients.$lines.dat
else
	unzip -p $ibase/pdoptcards2016093000000001.dat.zip | \
	       head -$lines > $ibase/clients.$lines.dat
fi

if [ $lines -eq 0 ]
then
	unzip -p $ibase/pdoptstores2016093000000001.dat.zip > \
	         $ibase/stores.$lines.dat
else
	unzip -p $ibase/pdoptstores2016093000000001.dat.zip | \
	       head -$lines > $ibase/stores.$lines.dat
fi

echo "family;category;subcat;product_key;product_desc;brand;product_type;brand_type;product_weight" > $obase/products.csv
tail -n+2 $ibase/products.$lines.dat | \
     cut --output-delimiter=";" -d"|" -f11,12,6,8,10,14,16,17,20 \
     >> $obase/products.csv

echo "store_key;store_name;store_group;cluster_area;store_type;zip_code;region;director;manager;sales_area;coord_x;coord_y" > $obase/stores.csv
tail -n+2 $ibase/stores.$lines.dat | \
     cut --output-delimiter=";" -d "|" -f1,2,4,5,6,8,10,11,12,13,17,18 \
     >> $obase/stores.csv

echo "account_id;card_key;age;gender;city;province;country;registration;status;household_members;email;phone;mobile" > $obase/clients.csv
tail -n+2 $ibase/clients.$lines.dat | \
     cut --output-delimiter=";" -d"|" -f1,2,3,4,5,8,9,10,11,13,14,15,16 | csvmagic \
     >> $obase/clients.csv
