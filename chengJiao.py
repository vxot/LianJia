from datetime import date
import sys
from datetime import date

from bs4 import BeautifulSoup

import utils
from createTable import ChengJiao
from session import LianJiaSession


class ChengJiaoHouse:

    def __init__(self, lian_jia_session):
        self.__lian_jia_session = lian_jia_session
        self.base_url = self.__lian_jia_session.get_city_url()
        self.__logger = self.__lian_jia_session.get_logger()
        self.sql_session = self.__lian_jia_session.get_sql_session()

    def parse(self):
        url = '{0}/chengjiao/'.format(self.base_url)
        rep = self.__lian_jia_session.get(url)
        soup = BeautifulSoup(rep.text, 'lxml')
        total = soup.find('div', attrs={'class', 'total'}).find('span').get_text(strip=True)
        total = int(total)
        page_url_list = utils.get_all_page(soup)
        self.__logger.info('发现总房源{0}套'.format(total))
        self.__parse_soup(soup)
        i = 0
        t = len(page_url_list)
        for url in page_url_list:
            i += 1
            self.__logger.info('progress {0}'.format(i/t))
            rep = self.__lian_jia_session.get(self.base_url + url)
            soup = BeautifulSoup(rep.text, 'lxml')
            self.__parse_soup(soup)

    def __parse_soup(self, soup):
        li_arr = soup.select('ul.listContent li')
        cheng_jiao_list = []
        for li in li_arr:
            cheng_jiao = ChengJiao()
            info_div = li.find('div', attrs={'class', 'info'})
            url, title = utils.get_url_title(info_div)

            price = info_div.find('div', attrs={'class', 'totalPrice'}).find('span', attrs={'class', 'number'}).get_text(strip=True)
            unit_price = info_div.find('div', attrs={'class', 'unitPrice'}).find('span', attrs={'class', 'number'}).get_text(strip=True)
            cheng_jiao.price = float(price)
            cheng_jiao.unit_price = float(unit_price)
            deal_date = info_div.find('div', attrs={'class', 'dealDate'}).get_text(strip=True)
            y_m_d = deal_date.split('.')
            deal_date = date(int(y_m_d[0]), int(y_m_d[1]), int(y_m_d[2]))
            deal_cycle_txt = info_div.find('span', attrs={'class', 'dealCycleTxt'}).get_text(strip=True)
            flood = info_div.find('div', attrs={'class', 'positionInfo'}).get_text(strip=True)
            gua_pai_jia = deal_cycle_txt[deal_cycle_txt.find('牌') + 1:deal_cycle_txt.find('万')]
            gua_pai_jia = float(gua_pai_jia)
            zhou_qi = deal_cycle_txt[deal_cycle_txt.find('期') + 1:deal_cycle_txt.find('天')]
            zhou_qi = int(zhou_qi)
            cheng_jiao.url = url
            cheng_jiao.title = title
            cheng_jiao.deal_date = deal_date
            cheng_jiao.gua_pai_jia = gua_pai_jia
            cheng_jiao.zhou_qi = zhou_qi
            cheng_jiao.flood = flood
            cheng_jiao_list.append(cheng_jiao)
            self.__logger.info('url[{0} title{1} 价格[{2}]]'.format(url, title, gua_pai_jia))
        self.sql_session.add_all(cheng_jiao_list)
        self.sql_session.commit()


if '__main__' == __name__:
    if len(sys.argv) == 2:
        filename, city = sys.argv
        lian_jia_session = LianJiaSession(city)
    else:
        lian_jia_session = LianJiaSession()
    chengJiaoHouse = ChengJiaoHouse(lian_jia_session)
    chengJiaoHouse.parse()
