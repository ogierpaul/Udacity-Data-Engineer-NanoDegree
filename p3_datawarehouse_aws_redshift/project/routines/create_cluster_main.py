from p3_datawarehouse_aws_redshift.src import setup_main
import configparser

# Input here the path to the admin config file
config_path = '../config/admin_config.cfg'

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read_file(open(config_path))
    setup_main(config)
    pass

