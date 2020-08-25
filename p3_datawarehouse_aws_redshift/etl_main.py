from p3_datawarehouse_aws_redshift.src import etl_main
import configparser
config = configparser.ConfigParser()
config.read('dwh.cfg')

if __name__ == '__main__':
    etl_main(config)
