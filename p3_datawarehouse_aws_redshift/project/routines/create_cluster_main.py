from p3_datawarehouse_aws_redshift.p3src import create_cluster_main
import configparser

# Input here the path to the admin config file
config_path = '/Users/paulogier/81-GithubPackages/Udacity-Data-Engineer-NanoDegree/p3_datawarehouse_aws_redshift/project/config/admin_config.cfg'

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read_file(open(config_path))
    create_cluster_main(config)
    pass

