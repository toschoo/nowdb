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

echo "family;category;subcat;product_key;product_desc;brand;product_type;brand_type;product_weight" > $obase/products.csv
tail -n+2 $ibase/products.100.dat | \
     cut --output-delimiter=";" -d"|" -f11,12,6,8,10,14,16,17,20 \
     >> $obase/products.csv

echo "store_key;store_name;store_group;cluster_area;store_type;zip_code;region;director;manager;sales_area;coord_x;coord_y" > $obase/stores.csv
tail -n+2 $ibase/stores.100.dat | \
     cut --output-delimiter=";" -d "|" -f1,2,4,5,6,8,10,11,12,13,17,18 \
     >> $obase/stores.csv

echo "account_id;card_key;age;gender;city;province;country;registration;status;household_members;email;phone;mobile" > $obase/clients.csv
tail -n+2 $ibase/clients.100.dat | \
     cut --output-delimiter=";" -d"|" -f1,2,3,4,5,8,9,10,11,13,14,15,16 | csvmagic \
     >> $obase/clients.csv
