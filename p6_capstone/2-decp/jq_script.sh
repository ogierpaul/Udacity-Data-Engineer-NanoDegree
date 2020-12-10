echo 'starting parsing script'

export inputpath=/home/ec2-user/data/decp.json
export jqmarches=/home/ec2-user/data/jq_transfo_marches.sh
export jqtitulaires=/home/ec2-user/data/jq_transfo_titulaires.sh
export outputtemp=/home/ec2-user/data/temp_marches.json
export outputmarches=/home/ec2-user/data/marches.json
export outputtitulaires=/home/ec2-user/data/titulaires.json

mkdir /home/ec2-user/data
cd /home/ec2-user/data
wget https://www.data.gouv.fr/fr/datasets/r/16962018-5c31-4296-9454-5998585496d2 -O decp.json


sudo yum install jq -y

cat $inputpath  | jq -cr '.marches[]' | sed 's/\\[tn]//g' > $outputtemp
head -5 $outputtemp



echo 'PWD: ' $PWD

echo 'Exporting marches'
aws s3 cp s3://paulogiereucentral1/p6/input/jq_transfo_marches.sh /home/ec2-user/data/jq_transfo_marches.sh
echo 'Showing jqpath' $jqmarches
head -5 $jqmarches
cat $outputtemp \
| jq --compact-output\
 "`cat $jqmarches`" \
 > $outputmarches
echo 'Showing outputpath' $outputmarches
head -5 $outputmarches
aws s3 cp  $outputmarches s3://paulogiereucentral1/p6/decp/output/marches.json



echo 'Exporting titulaires'
aws s3 cp s3://paulogiereucentral1/p6/input/jq_transfo_titulaires.sh /home/ec2-user/data/jq_transfo_titulaires.sh
head -5 $jqtitulaires
cat $outputtemp \
| jq --compact-output\
 "`cat $jqtitulaires`" \
 > $outputtitulaires
echo 'Showing outputpath' $outputtitulaires
head -5 $outputtitulaires
aws s3 cp  $outputtitulaires s3://paulogiereucentral1/p6/decp/output/titulaires.json

#TODO: Add script to extract acheteur_id and acheteur_name

#TODO: Add also script to download SIREN Data
wget https://www.data.gouv.fr/en/datasets/r/a5bdbdaf-0927-499b-b11e-e293a329d39f -O siren.zip
unzip siren.zip
aws s3 cp StockUniteLegale_utf8.csv  s3://paulogiereucentral1/p6/siren/input/StockUniteLegale_utf8.csv


echo 'done'

