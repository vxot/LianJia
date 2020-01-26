import logging
import os
from datetime import datetime

import requests
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

login_url = 'https://upassport.lianjia.com/login'
properties_filse = 'properties.yml'


class LianJiaSession:

    def __init__(self, city=None):
        with open(properties_filse, 'r', encoding="utf-8") as f:
            self.yaml_data = yaml.load(f.read(), Loader=yaml.FullLoader)
        if city is None:
            self.city = self.yaml_data['city']
        else:
            self.city = city
        self.city_zh = self._get_city_zh()
        self.__logger_name = 'lianjia'
        self.__init_logger()
        self.__headers = self.get_headers()
        self.__logger = logging.getLogger(self.__logger_name)
        self.__web_session = self.__login()
        self.__logger.info('开始爬城市[{0}]'.format(self.city_zh))

    def get(self, url):
        try:
            rep = self.__web_session.get(url, headers=self.__headers)
            return rep
        except Exception as e:
            self.__logger.error(e)

    def __login(self):
        try:
            web_session = requests.Session()
            ori_data = 'username=aa&password=bb&code=&' \
                       'service=https%3A%2F%2Fajax.api.lianjia.com%2Flogin%2Flogin%2Fgetuserinfo&' \
                       'isajax=true&_eventId=submit&remember=1&' \
                       'lt=LT-159472-MJo29Q6dVcEP1Nk3R00jzzGghfKdTd-upassport.lianjia.com&execution=e2s1'
            ori_data_arr = ori_data.split('&')
            data = {}
            for item in ori_data_arr:
                key_value = item.split('=')
                data[key_value[0]] = key_value[1]
            web_session.post(login_url, data, headers=self.__headers)
        except Exception:
            self.__logger.error("登录失败！")
            return None
        return web_session

    def __get_city_conf(self):
        if 'cities' in self.yaml_data and self.city in self.yaml_data['cities']:
            city_conf = self.yaml_data['cities'][self.city]
        else:
            raise RuntimeError('请添加城市[{0}]配置文件'.format(self.city))
        return city_conf

    def get_headers(self):
        referer = '{0}/ershoufang/'.format(self.get_city_url())
        user_agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'
        headers = {'User-Agent': user_agent, 'Referer': referer}
        return headers

    def get_city_url(self):
        city_conf = self.__get_city_conf()
        return city_conf['url']

    def _get_city_zh(self):
        city_conf = self.__get_city_conf()
        return city_conf['zh']

    def get_city_zh(self):
        return self.city_zh

    def get_sql_engine(self):
        city_conf = self.__get_city_conf()
        sql_url = 'mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'.format(
            city_conf['db.username'], city_conf['db.passwort'],
            city_conf['db.hostname'], city_conf['db.port'], city_conf['db.database'])
        engine = create_engine(sql_url, encoding='utf-8', echo=False)
        return engine

    def get_sql_session(self):
        engine = self.get_sql_engine()
        Session_class = sessionmaker(bind=engine)  # 创建与数据库的会话，class,不是实例
        sql_session = Session_class()  # 生成session实例
        return sql_session

    def get_prop(self):
        return self.yaml_data

    def get_prop_value(self, key):
        return self.yaml_data[key]

    def get_logger(self):
        return logging.getLogger(self.__logger_name)

    def __get_log_path(self):
        now = datetime.now()
        time_str = now.strftime("%Y%m%d")
        log_path = os.path.join(os.getcwd(), 'logs')
        if 'log_path' in self.yaml_data:
            log_path = self.yaml_data['log_path']
        if not os.path.exists(log_path):
            os.mkdir(log_path)
        self.__log_path = os.path.join(log_path, time_str)
        if not os.path.exists(self.__log_path):
            os.mkdir(self.__log_path)

    def get_log_path(self):
        return self.__log_path

    def get_log_file_name(self):
        return self.log_file_name

    def __init_logger(self):
        self.__get_log_path()
        log_path = self.__log_path
        now = datetime.now()
        time_str = now.strftime("%H%M")
        self.log_file_name = os.path.join(log_path, time_str + '_' + self.city + '_log.log')
        # 创建一个logger
        my_logger = logging.getLogger(self.__logger_name)

        # 指定日志的最低输出级别，默认为WARN级别
        my_logger.setLevel(logging.INFO)

        # 创建一个handler用于写入日志文件
        file_handler = logging.FileHandler(self.log_file_name, encoding='utf-8')

        # 创建一个handler用于输出控制台
        console_handler = logging.StreamHandler()

        # 定义handler的输出格式
        formatter = logging.Formatter('[%(asctime)s] - %(levelname)s: %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # 给logger添加handler
        my_logger.addHandler(file_handler)
        my_logger.addHandler(console_handler)


if '__main__' == __name__:
    session = LianJiaSession('wuhan')
    prop = session.get_prop()
    print(prop)
    city_url = session.get_city_url()
    print(city_url)
