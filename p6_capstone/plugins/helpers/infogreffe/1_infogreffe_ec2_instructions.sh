cd {ec2dir}
rm -rf {ec2dir}/*
curl -X GET "{url}" -o {fname_ec2}
aws s3 cp {fname_ec2} {fname_s3}
rm -rf {ec2dir}/*