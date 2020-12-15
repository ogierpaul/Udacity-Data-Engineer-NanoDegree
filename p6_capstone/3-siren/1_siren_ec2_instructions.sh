# Only write commands on one line as the file will be split with \n
# Clean directory before launching
cd {ec2dir}
rm -rf {ec2dir}/*
# Download file and unzip it
wget {url} -O {input_file_ec2}
unzip {input_file_ec2}
# Copy to s3
aws s3 cp {csvname} {output_siren_s3}
# Clean up
rm -rf /{ec2dir}/*