#TODO: update default security group to accept ingress from myip at least on redshift port and ssh
#TODO: create a security group for redshift to accept ingress from public web, only on specific port
#TODO: Get id of those two security groups
# #TODO: Create IAM Role for: access rw to s3 (redshift, ec2), ssm to ec2 & get arn/rolenames
# - Create an IAM Role for Redshift access to S3 for redshift_user
# - Create an IAM Role for Redshift access, for ro_user
# - Create an IAM Role for EC2 access to S3, SSM
# - Create an IAM Role for EMR Spark Cluster
