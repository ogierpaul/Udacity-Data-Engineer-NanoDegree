# Only write commands on one line as the file will be split with \n
# Clean directory before launching
cd /home/ec2-user
rm -rf /home/ec2-user/*
# Download file and unzip it
wget {url} -O /home/ec2-user/siren.csv
unzip /home/ec2-user/siren.csv
# Copy to s3
aws s3 cp {csvname} {output_s3}
# Clean up
rm -rf /home/ec2-user/*