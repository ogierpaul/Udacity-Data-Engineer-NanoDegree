from p3src import etl_main
import configparser
# Input here path to the db user config file
config_path = '/Users/paulogier/81-GithubPackages/Udacity-Data-Engineer-NanoDegree/p3_datawarehouse_aws_redshift/project/config/dbuser_config.cfg'

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read(config_path)
    etl_main(config)
