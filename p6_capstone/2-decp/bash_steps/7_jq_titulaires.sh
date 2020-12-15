cat $outputtemp \
| jq --compact-output\
 "`cat $jqtitulaires`" \
 > $outputtitulaires