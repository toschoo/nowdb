p=$(ps -fu nowdb | grep nowdbd | grep -v grep | awk '{print $2}')
kill $p
