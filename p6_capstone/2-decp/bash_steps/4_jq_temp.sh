cat $inputfile  | jq -cr '.marches[]' | sed 's/\\[tn]//g' > $outputtemp
