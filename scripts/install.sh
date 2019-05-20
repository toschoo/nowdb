if [ $# -lt 1 ]
then
  echo "no target path"
  exit 1
fi

path="$1"

echo $path

if [ -z "$path" ]
then
  echo "target path is empty"
  exit 1
fi

if [ ! -d $path ]
then
  echo "target path does not exist"
  exit 1
fi

if [ ! -d $path/lua ]
then
  echo "making path $path/lua"
  mkdir $path/lua
fi

cp lua/nowdb.lua $path/lua/
