#cat $outputtemp \
#| jq --compact-output\
# "`cat $jqmarches`" \
# > $outputmarches

cat {outputtemp} \
| jq --compact-output\
 "`cat {jqmarches}`" \
 > {outputmarches}

