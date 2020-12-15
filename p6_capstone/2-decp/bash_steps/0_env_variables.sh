#Input variables
export jq_marches_s3={jq_marches_s3}
export jq_titulaires_s3={jq_titulaires_s3}
export adress={address}
# Internal Variables
export inputfile=/home/ec2-user/decp.json
export jqmarches=/home/ec2-user/jq_marches.sh
export jqtitulaires=/home/ec2-user/jq_titulaires.sh
export outputtemp=/home/ec2-user/temp_marches.json
export outputmarches=/home/ec2-user/marches.json
export outputtitulaires=/home/ec2-user/titulaires.json
# Output variables
export decp_titulaires_s3={decp_titulaires_s3}
export decp_marches_s3={decp_marches_s3}
