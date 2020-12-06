cat sample.json \
| jq --compact-output\
 "`cat jqpath_marches.json`" \
 > marches.json

cat sample.json \
| jq --compact-output\
 "`cat jqpath_titulaires.json`" \
 > titulaires.json
