import queue
import sys
from datetime import date

from bs4 import BeautifulSoup
import re
import utils
from createTable import ChengJiao, XiaoQu
from session import LianJiaSession


class ChengJiaoHouse:

    def __init__(self, city=None):
        self.lian_jia_session = LianJiaSession(city)
        self.__yaml_data = self.lian_jia_session.get_prop()
        self.base_url = self.lian_jia_session.get_city_url()
        self.__logger = self.lian_jia_session.get_logger()
        self.sql_session = self.lian_jia_session.get_sql_session()

    def test(self, is_breaking=False):
        xiao_qus = self.sql_session.query(XiaoQu).filter(XiaoQu.status == False).filter(XiaoQu.zai_shou >= self.__yaml_data['min_house']).all()
        for xiao_qu in xiao_qus:
            cjs = self.sql_session.query(ChengJiao).filter(ChengJiao.xiao_qu == xiao_qu.id).all()
            if len(cjs) > 0:
                xiao_qu.status = True
        self.sql_session.commit()

    def parse(self, is_breaking=False):
        utils.reset_xiao_qu_status(self.sql_session, is_breaking)
        xiao_qu_queue = self.__get_xiao_qu_map()
        t = xiao_qu_queue.qsize()
        i = 0
        while not xiao_qu_queue.empty():
            i += 1
            self.__logger.info('总进度 ===> {:0.2f}'.format(i / t))
            xiao_qu = xiao_qu_queue.get()
            if xiao_qu is None:
                break
            url = 'https://wh.lianjia.com/chengjiao/c{0}/'.format(xiao_qu.url)
            self.parse_page(url, xiao_qu.id)
            xiao_qu.status = True
            self.sql_session.commit()

    def parse_latest(self):
        url = '{0}/chengjiao/'.format(self.base_url)
        self.parse_page(url)

    def parse_page(self, url, xiao_qu_id=None):
        rep = self.lian_jia_session.get(url)
        soup = BeautifulSoup(rep.text, 'lxml')
        total = soup.find('div', attrs={'class', 'total'}).find('span').get_text(strip=True)
        total = int(total)
        if total > 1000:
            self.__logger.info('error 小区[{0}] 发现房源数异常[{1}]'.format(url, total))
        else:
            self.__logger.info('发现总房源{0}套'.format(total))
            page_url_list = utils.get_all_page(soup)
            self.__parse_soup(soup, xiao_qu_id)
            # i = 0
            # t = len(page_url_list)
            for url in page_url_list:
                # i += 1
                # self.__logger.info('当前小区 progress {:0.2f}'.format(i/t))
                rep = self.lian_jia_session.get(self.base_url + url)
                soup = BeautifulSoup(rep.text, 'lxml')
                self.__parse_soup(soup, xiao_qu_id)

    def __get_xiao_qu_id_by_name(self, name):
        xiao_qus = self.sql_session.query(XiaoQu).filter(XiaoQu.name == name).all()
        if len(xiao_qus) == 1:
            return xiao_qus[0].id
        elif len(xiao_qus) > 1:
            self.__logger.info('error 找多个同名小区 名称[{0}]'.format(name))
            return None
        else:
            self.__logger.info('error 找不到小区 名称[{0}]'.format(name))
            return None

    def __parse_soup(self, soup, xiao_qu_id):
        li_arr = soup.select('ul.listContent li')
        cheng_jiao_list = []
        for li in li_arr:
            info_div = li.find('div', attrs={'class', 'info'})
            url, title = utils.get_url_title(info_div)
            exist = self.sql_session.query(ChengJiao).filter(ChengJiao.url == url).one_or_none()
            # 如果 已存在跳过
            if exist is None:
                cheng_jiao = ChengJiao()
                args = title.split(' ')
                if len(args) == 3:
                    if xiao_qu_id is None:
                        xiao_qu_id = self.__get_xiao_qu_id_by_name(args[0])
                    cheng_jiao.xiao_qu = xiao_qu_id
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
                    cheng_jiao.hu_xing = args[1]
                    cheng_jiao.area1 = float(re.findall(r"\d+\.?\d*", args[2])[0])
                    cheng_jiao_list.append(cheng_jiao)
                    self.__logger.info('url[{0}] 标题[{1}] 价格[{2}] 单价[{3}] 成交日期[{4}]'.format(
                        url, title, gua_pai_jia, unit_price, deal_date))
            self.sql_session.add_all(cheng_jiao_list)
        self.sql_session.commit()

    def __get_xiao_qu_map(self):
        xiao_qus = self.sql_session.query(XiaoQu).filter(XiaoQu.status == False).filter(XiaoQu.zai_shou >= self.__yaml_data['min_house']).all()
        xiao_qu_quenue = queue.Queue()
        for item in xiao_qus:
            xiao_qu_quenue.put(item)
        self.__logger.info('发现小区数量 : [{0}](小区房源数量至少{1})'.format(xiao_qu_quenue.qsize(), self.__yaml_data['min_house']))
        return xiao_qu_quenue


if '__main__' == __name__:
    if len(sys.argv) == 2:
        filename, city = sys.argv
        chengJiaoHouse = ChengJiaoHouse(city)
    else:
        chengJiaoHouse = ChengJiaoHouse()
    chengJiaoHouse.parse(True)
