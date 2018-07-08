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

echo "FAMILY;CATEGORY;SUBCAT;PRODUCT_KEY;PRODUCT_DESC;BRAND;PRODUCT_TYPE;BRAND_TYPE;PRODUCT_WEIGHT" > $obase/products.csv
tail -n+2 $ibase/pdoptproducts.100.dat | \
     cut --output-delimiter=";" -d"|" -f11,12,6,8,10,14,16,17,20 \
     >> $obase/products.csv

echo "STORE_KEY;STORE_NAME;STORE_GROUP;CLUSTER_AREA;STORE_TYPE;ZIP_CODE;REGION;DIRECTOR;MANAGER;SALES_AREA;COORD_X;COORD_Y" > $obase/stores.csv
tail -n+2 $ibase/pdoptstores.100.dat | \
     cut --output-delimiter=";" -d "|" -f1,2,4,5,6,8,10,11,12,13,17,18 \
     >> $obase/stores.csv

echo "ACCOUNT_ID;CARD_KEY;AGE;GENDER;CITY;PROVINCE;COUNTRY;REGISTRATION;STATUS;HOUSEHOLDMEMBERS;E_MAIL;PHONE;MOBILE_PHONE" > $obase/clients.csv
tail -n+2 $ibase/pdoptcards.100.dat | \
     cut --output-delimiter=";" -d"|" -f1,2,3,4,5,8,9,10,11,13,14,15,16 \
     >> $obase/clients.csv
