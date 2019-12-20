import re
import threading
from datetime import datetime, date
from bs4 import BeautifulSoup
import utils
import os
import traceback
from createTable import House, PriceChange, PriceChangeNot, HouseNot, ChengJiao, XiaoQu

re_null = re.compile(r'\n|&nbsp|\xa0|\\xa0|\u3000|\\u3000|\\u0020|\u0020|\t|\r')


class XiaoQuHouses(threading.Thread):

    def __init__(self, lian_jia_session, xiao_qu_queue, xiao_qu_id_soup_queue):
        threading.Thread.__init__(self)
        self.__xiao_qu_queue = xiao_qu_queue
        self.__xiao_qu_id_soup_queue = xiao_qu_id_soup_queue
        self.__lian_jia_session = lian_jia_session
        self.min_house = self.__lian_jia_session.get_prop_value('min_house')
        self.__logger = self.__lian_jia_session.get_logger()
        self.sql_session = self.__lian_jia_session.get_sql_session()
        self.__base_url = self.__lian_jia_session.get_city_url()

    def run(self):
        while True:
            if self.__xiao_qu_queue.empty():
                break
            self.__get_house_list()
        self.__logger.info('{0} ==> completed'.format(self.getName()))

    def __get_house_list(self):
        try:
            self.__logger.info('{0} ==> 剩余小区数  ： {1}'.format(self.getName(), self.__xiao_qu_queue.qsize()))
            xiao_qu = self.__xiao_qu_queue.get()
            url = '{0}/ershoufang/c{1}'.format(self.__base_url, xiao_qu.url)
            rep = self.__lian_jia_session.get(url)
            if rep is not None:
                soup = BeautifulSoup(rep.text, 'lxml')
                total = soup.find('h2', attrs={'class', 'total'}).find('span').get_text(strip=True)
                total = int(total)
                # 更新小区在售房源数(其它数据没更新)
                if xiao_qu.zai_shou != total:
                    self.__updata_zai_shou(total, xiao_qu.id)
                if total < self.min_house:
                    self.__logger.info('小区房源[{0}({3})] , 数量({2})太少，跳过] ===> url[{1}] '.format(xiao_qu.name, xiao_qu.url, total, xiao_qu.id))
                elif total > 800:
                    self.__logger.error('小区房源[id: {0}] , 数量({2})异常 ===> url[{1}] '.format(xiao_qu.name, xiao_qu.url, total))
                else:
                    self.__logger.info('开始爬取小区[{0}] ===> url[{1}]，共发现房源数 : {2}'.format(xiao_qu.name, xiao_qu.url, total))
                    soup_arr = [soup]
                    page_url_list = utils.get_all_page(soup)
                    for url in page_url_list:
                        soup = self.__parse_other_page(url)
                        if soup is not None:
                            soup_arr.append(soup)
                    self.__xiao_qu_id_soup_queue.put((xiao_qu.id, soup_arr))
            else:
                self.__logger.error('rep is None,小区小区[{0}] url[{1}]'.format(xiao_qu.name, xiao_qu.url))
        except Exception:
            self.__logger.error(traceback.format_exc())

    def __parse_other_page(self, url):
        self.__logger.info('{0} start parse other page url[{1}]'.format(self.getName(), url))
        url = '{0}/ershoufang/c{1}'.format(self.__base_url, url)
        rep = self.__lian_jia_session.get(url)
        soup = BeautifulSoup(rep.text, 'lxml')
        return soup

    def __updata_zai_shou(self, total, xiao_qu_id):
        self.__logger.info('{2} 更新小区[{0}]在售房源 {1}]'.format(xiao_qu_id, total, self.getName()))
        self.sql_session.execute('update xiao_qu set zai_shou = {0} where id = {1}'.format(total, xiao_qu_id))
        self.sql_session.commit()


class ParseXiaoQuPage(threading.Thread):
    def __init__(self, lian_jia_session, xiao_qu_id_soup_queue):
        threading.Thread.__init__(self)
        self.__xiao_qu_id_soup_queue = xiao_qu_id_soup_queue
        self.__lian_jia_session = lian_jia_session
        self.__logger = self.__lian_jia_session.get_logger()
        self.sql_session = self.__lian_jia_session.get_sql_session()
        self.__base_url = self.__lian_jia_session.get_city_url()

    def run(self):
        while True:
            exist_house_map = {}
            new_house_list = []
            price_change_list = []
            xiao_qu_id, soup_arr = self.__xiao_qu_id_soup_queue.get()
            if soup_arr is None:
                break
            try:
                self.__logger.info('{0} ==> 剩余soup数 : {1} , 开始爬取小区[{2}], soup数量[{3}]'.format(
                    self.getName(), self.__xiao_qu_id_soup_queue.qsize(), xiao_qu_id, len(soup_arr)))
                exist_houses = self.sql_session.query(House).filter(House.xiao_qu == xiao_qu_id).all()
                for item in exist_houses:
                    exist_house_map[item.url] = item
                for soup in soup_arr:
                    self.__parse_page(xiao_qu_id, soup, exist_house_map, new_house_list, price_change_list)
                self.sql_session.add_all(price_change_list)
                self.sql_session.add_all(new_house_list)
                self.sql_session.commit()
                houses = self.sql_session.query(House).filter(House.xiao_qu == xiao_qu_id).filter(House.status == False).all()
                for house in houses:
                    self.__deal_house(house.url)
                # 断点，记录小区 已经 爬过
                xiao_qu = self.sql_session.query(XiaoQu).filter(XiaoQu.id == xiao_qu_id).one_or_none()
                xiao_qu.status = True
                self.sql_session.commit()
            except Exception:
                self.__logger.error(self.__logger.error(traceback.format_exc()))
        self.__logger.info('{0} ==> consumer_num completed'.format(self.getName()))

    def __parse_page(self, xiao_qu_id, soup, exist_house_map, new_house_list, price_change_list):
        # 新房
        url_set = set()
        exist_set = set()
        li_arr = soup.select('ul.sellListContent li')
        exist_urls = exist_house_map.keys()
        for li in li_arr:
            div = li.div
            url, title = utils.get_url_title(div)
            price_text = div.find('div', attrs={'class', 'totalPrice'}).get_text(strip=True)
            price = price_text[0:len(price_text) - 1]
            price = float(price)
            unit_price_text = div.find('div', attrs={'class', 'unitPrice'}).get_text(strip=True)
            unit_price_text = unit_price_text[unit_price_text.find('价')+1:unit_price_text.find('元')]
            unit_price = float(unit_price_text)
            if url in exist_urls:
                exist_set.add(url)
                exist_house = exist_house_map.get(url)
                price_change = self.__check_price(exist_house, price, unit_price)
                if price_change is not None:
                    price_change_list.append(price_change)
            elif url not in url_set:
                url_set.add(url)
                new_house = self.__init_new_house(div, url, price, unit_price, title, xiao_qu_id)
                if new_house is not None:
                    new_house_list.append(new_house)
            else:
                self.__logger.info('error ===========================异常房源url {0}'.format(url))
        if len(li_arr) != len(exist_set) + len(url_set):
            self.__logger.error('error 小区[{0}]解析数量错误'.format(xiao_qu_id))
        if len(li_arr) == 0:
            file_path = self.__lian_jia_session.get_log_path()
            now = datetime.now()
            time_str = now.strftime("%H%M")
            xiao_qu_html = os.path.join(file_path, str(xiao_qu_id) + '_' + time_str + '.html')
            with open(xiao_qu_html, 'w', encoding='utf-8') as f:
                f.write(soup.prettify())
        self.__logger.info('小区[{5}]该页li个数[{0}]检测旧房源{1}[{2}]\n发现新房源{3}[{4}]'.format(len(li_arr), len(exist_set), ','.join(exist_set), len(url_set), ','.join(url_set), xiao_qu_id))

    def __init_new_house(self, div, url, price, unit_price, title, xiao_qu_id):
        address_text = div.find('div', attrs={'class', 'address'}).get_text(strip=True)
        address_text = re_null.sub('', address_text)
        address_arr = address_text.split('|')
        exist_house = self.sql_session.query(House).filter(House.url == url).one_or_none()
        if exist_house is not None:
            self.__check_price(exist_house, price, unit_price)
        elif '车位' not in address_arr:
            house = House()
            house.url = url
            house.title = title
            follow_text = div.find('div', attrs={'class', 'followInfo'}).get_text(strip=True)
            house.price = price
            house.unit_price = unit_price
            house.star = int(follow_text[0:follow_text.index('人')])

            if len(address_arr) != 7:
                self.__logger.error('特殊情况，没处理。url[{0}]error: {1}'.format(url, address_text))
                # house.flood = address_arr[1]
                # del address_arr[1]
            house.xiao_qu = xiao_qu_id
            house.hu_xing = address_arr[0]
            house.area = float(re.findall(r"\d+\.?\d*", address_arr[1])[0])
            house.chao_xiang = address_arr[2]
            house.zhuang_xiu = address_arr[3]
            house.flood = address_arr[4]
            house.create_time = datetime.now()
            # 标记，以免误认为是删除的房源
            house.status = True
            self.__logger.info('new house : title[{0}] , url[{1}]'.format(house.title, house.url))
            return house

    def __check_price(self, house_exist, price, unit_price):
        house_exist.status = True
        if price != house_exist.price:
            price_change = PriceChange()
            price_change.house_id = house_exist.id
            price_change.pre_price = house_exist.price
            price_change.price = price
            price_change.change_time = datetime.now()
            house_exist.price = price
            house_exist.unit_price = unit_price
            self.__logger.info('房源[{0}] 挂牌价变动! [{1} ===> {2}]'.format(house_exist.url, price_change.pre_price, price_change.price))
            return price_change

    def __deal_price_change_not(self, ori_house, house_not_id):
        price_change_list = self.sql_session.query(PriceChange).filter(PriceChange.house_id == ori_house.id).all()
        for priceChange in price_change_list:
            price_change_not = PriceChangeNot()
            price_change_not.pre_price = priceChange.pre_price
            price_change_not.price = priceChange.price
            price_change_not.house_not_id = house_not_id
            price_change_not.change_time = priceChange.change_time
            price_change_not.create_time = datetime.now()
            self.sql_session.delete(priceChange)
            self.sql_session.add(price_change_not)
        self.__logger.info('{0} 删除房子[{1}]'.format(self.getName(), ori_house.url))
        self.sql_session.commit()
        self.sql_session.delete(ori_house)
        self.sql_session.commit()

    def __deal_house(self, url):
        url2 = '{0}/ershoufang/{1}.html'.format(self.__base_url, url)
        rep = self.__lian_jia_session.get(url2)
        if rep.status_code == 404:
            self.__logger.info('house[{0}] 404'.format(url))
            self.__deal_not(url)
        elif '/chengjiao/' in rep.url:
            self.__logger.info('house[{0}] 成交'.format(url))
            self.__deal_cheng_jiao(rep, url)
        else:
            soup = BeautifulSoup(rep.text, 'lxml')
            h1 = soup.find('h1', attrs={'class', 'main'})
            if h1 is None:
                self.__logger.info(str(url) + 'h1 is none ==========================================')
            else:
                span = h1.find('span')
                if span is not None:
                    span_text = span.get_text(strip=True)
                    if span_text == '已下架':
                        self.__logger.info('house[{0}] 已下架'.format(url))
                        self.__deal_not(url)
                    else:
                        self.__logger.info('house[{0}] ?????'.format(url))
                else:
                    self.__logger.info('house[{0}] span is none'.format(url))

    def __deal_cheng_jiao(self, rep, url):
        self.__logger.info('处理成交 : {0}'.format(url))
        cheng_jiao = ChengJiao()
        cheng_jiao.url = url
        not_exist = self.sql_session.query(House).filter(House.url == url).one_or_none()
        if not_exist is not None:
            cheng_jiao.ori_id = not_exist.id
        soup = BeautifulSoup(rep.text, 'lxml')
        span = soup.find('div', attrs={'class', 'house-title'}).find('div', attrs={'class', 'wrapper'}).find('span')
        if span is not None:
            span_text = span.get_text(strip=True)
            chen_jiao_time = span_text[:10]
            y_m_d = chen_jiao_time.split('.')
            chen_jiao_date = date(int(y_m_d[0]), int(y_m_d[1]), int(y_m_d[2]))
            cheng_jiao.deal_date = chen_jiao_date
        div = soup.find('div', attrs={'class', 'info'})

        price_div = div.find('div', attrs={'class', 'price'})
        price_span = price_div.find('span', attrs={'class', 'dealTotalPrice'}).find('i')
        unit_price_span = price_div.find('b')
        cheng_jiao.price = float(price_span.get_text(strip=True))
        cheng_jiao.unit_price = float(unit_price_span.get_text(strip=True))
        msg_span_arr = div.find('div', attrs={'class', 'msg'}).find_all('span')
        for msg_span in msg_span_arr:
            text = msg_span.get_text(strip=True)
            print(text)
            if '挂' in text:
                text = text[:text.find('挂')]
                cheng_jiao.gua_pai_jia = float(text)
            elif '成' in text:
                text = text[:text.find('成')]
                cheng_jiao.zhou_qi = float(text)
            elif '调' in text:
                text = text[:text.find('调')]
                cheng_jiao.tiao_jia = float(text)
            elif '带' in text:
                text = text[:text.find('带')]
                cheng_jiao.dai_kan = float(text)
            elif '关' in text:
                text = text[:text.find('关')]
                cheng_jiao.star = float(text)
            elif '浏' in text:
                text = text[:text.find('浏')]
                cheng_jiao.liu_lan = float(text)
        self.sql_session.add(cheng_jiao)
        # 提交事务，生成id
        self.sql_session.commit()
        if not_exist is not None:
            self.__deal_price_change_not(not_exist, cheng_jiao.id)

    def __deal_not(self, url):
        not_exist = self.sql_session.query(House).filter(House.url == url).one()
        house_not = ParseXiaoQuPage.get_house_not(not_exist)
        self.sql_session.add(house_not)
        self.__deal_price_change_not(not_exist, house_not.id)
        self.sql_session.commit()

    @staticmethod
    def get_house_not(house):
        house2 = HouseNot()
        house2.ori_id = house.id
        house2.title = house.title
        house2.xiao_qu = house.xiao_qu
        house2.zhuang_xiu = house.zhuang_xiu
        house2.url = house.url
        house2.price = house.price
        house2.unit_price = house.unit_price
        house2.chao_xiang = house.chao_xiang
        house2.create_time = house.create_time
        house2.star = house.star
        house2.hu_xing = house.hu_xing
        house2.area = house.area
        house2.flood = house.flood
        house2.end_time = datetime.now()
        return house2


if __name__ == '__main__':
    # web_session = session.LianJiaSession().login()
    # cheng_jiao = ChenJiao2(web_session)
    # cheng_jiao.parse()
    # url = '104102319630'
    # rep = web_session.get('https://wh.lianjia.com/ershoufang/{0}.html'.format(url), headers=session.headers)
    # deal_cheng_jiao(rep, url)
    print('===')

