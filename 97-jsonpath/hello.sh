echo "Hello World"
echo $myenv
echo 'Input file at: ' $inputpath  "$(<$inputpath)"
echo $jqpath
echo $outputpath
cat $inputpath \
| jq --compact-output\
 "`cat $jqpath`" \
 > $outputpath
