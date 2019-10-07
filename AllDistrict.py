from bs4 import BeautifulSoup
from createTable import District
from session import LianJiaSession
import re
import sys

exclude_url = ['/xiaoqu/']
re_null = re.compile(r'\n|&nbsp|\xa0|\\xa0|\u3000|\\u3000|\\u0020|\u0020|\t|\r')


# 获取 区 信息
class AllDistrict:

    def __init__(self, city):
        self.__lian_jia_session = LianJiaSession(city)
        self.__yaml_data = self.__lian_jia_session.get_prop()
        self.__house_list = []
        self.__logger = self.__lian_jia_session.get_logger()
        self.__sql_session = self.__lian_jia_session.get_sql_session()
        self.__base_url = self.__lian_jia_session.get_city_url()
        # 为了防止名字相同，重复录入
        self.__district_names = set()

    def parse(self):
        a_arr = self.__parse_html('{0}/xiaoqu/'.format(self.__base_url))
        districts = self.__parse_a(a_arr)
        self.__sql_session.add_all(districts)
        self.__sql_session.commit()

        for item in districts:
            a_arr = self.__parse_html(self.__base_url + item.url)
            districts = self.__parse_a(a_arr, item.id)
            self.__sql_session.add_all(districts)
        self.__sql_session.commit()

    def __parse_html(self, url):
        rep = self.__lian_jia_session.get(url)
        soup = BeautifulSoup(rep.text, 'lxml')
        a_arr = soup.select('.m-filter .position a')
        return a_arr

    def __parse_a(self, items, parent_id=None):
        districts = []
        for item in items:
            if hasattr(item, 'href'):
                name = str(item.string)
                url = item['href']
                if name not in self.__district_names and url not in exclude_url:
                    district = District()
                    district.name = name
                    district.url = url
                    if parent_id:
                        district.parent = parent_id
                    self.__logger.info('add district name[{0}], url[{1}], parentId[{2}]'.format(name, url, parent_id))
                    districts.append(district)
                    self.__district_names.add(name)
        return districts


if __name__ == '__main__':
    filename, city = sys.argv
    AllDistrict(city).parse()
