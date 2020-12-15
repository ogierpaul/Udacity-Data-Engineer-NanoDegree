# Only write commands on one line as the file will be split with \n
# Install JQ and clean directory before launching
sudo yum install jq -y
cd {ec2dir}
rm -rf {ec2dir}/*
# Download file and parse with jq to transform it to JSON Lines format
wget {url} -O {input_file_ec2}
cat {input_file_ec2}  | jq -cr '.marches[]' | sed 's/\\[tn]//g' > {output_temp_ec2}
# Download jq parsing instructions to extract marches data, parse it and copy result to S3
aws s3 cp {jq_marches_s3} {jq_marches_ec2}
cat {output_temp_ec2} | jq --compact-output "`cat {jq_marches_ec2}`" > {output_marches_ec2}
aws s3 cp  {output_marches_ec2} {output_marches_s3}
# Same for titulaires
aws s3 cp {jq_titulaires_s3} {jq_titulaires_ec2}
cat {output_temp_ec2} | jq --compact-output "`cat {jq_titulaires_ec2}`" > {output_titulaires_ec2}
aws s3 cp  {output_titulaires_ec2} {output_titulaires_s3}
# Clean up
rm -rf /{ec2dir}/*

