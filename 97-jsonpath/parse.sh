echo 'starting parsing script'
echo 'PWD: ' $PWD

export inputpath='sample.json'
echo $inputpath
head -10 $inputpath

echo 'Exporting marches'
export jqpath='jqpath_marches.sh'
echo 'Showing jqpath' $jqpath
head -10 $jqpath
export outputpath='marches.json'

cat $inputpath \
| jq --compact-output\
 "`cat $jqpath`" \
 > $outputpath
echo 'Showing outputpath' $outputpath
head -10 $outputpath

echo 'Exporting titulaires'
export jqpath='jqpath_titulaires.sh'
head -10 $jqpath
export outputpath='titulaires.json'

cat $inputpath \
| jq --compact-output\
 "`cat $jqpath`" \
 > $outputpath
echo 'Showing outputpath' $outputpath
head -10 $outputpath


echo 'done'

