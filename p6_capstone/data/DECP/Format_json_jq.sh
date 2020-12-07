cat /data/21-sampledata/sample.json  | jq -cr '.marches[]' | sed 's/\\[tn]//g' > /data/21-sampledata/jq_sample.json
