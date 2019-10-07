import re
from bs4 import BeautifulSoup
import utils
from createTable import District, XiaoQu
from session import LianJiaSession
import sys

exclude_url = ['/xiaoqu/']
re_null = re.compile(r'\n|&nbsp|\xa0|\\xa0|\u3000|\\u3000|\\u0020|\u0020|\t|\r')


# 获district 所有小区
class AllXiaoQu:

    def __init__(self, city):
        self.__lian_jia_session = LianJiaSession(city)
        self.__yaml_data = self.__lian_jia_session.get_prop()
        self.__logger = self.__lian_jia_session.get_logger()
        self.__sql_session = self.__lian_jia_session.get_sql_session()
        self.__base_url = self.__lian_jia_session.get_city_url()
        
        self.__xiao_qu_urls = set()
        xiao_qus = self.__sql_session.query(XiaoQu).all()
        for item in xiao_qus:
            self.__xiao_qu_urls.add(item.url)

    def get_xiao_qu_list(self):
        i = 0
        district_list = self.__sql_session.query(District).filter(District.parent != None).all()
        t = len(district_list)/100
        for district in district_list:
            i += 1
            self.__logger.info('进度[{0:.2f}] 开始解析片区[{1}] id[{2}]===> url[{3}]'.format(
                i/t, district.name, district.id, district.url))
            xiao_qu_list = self.__parse_a_district(district)
            self.__sql_session.add_all(xiao_qu_list)
            self.__sql_session.commit()

    def __parse_a_district(self, district):
        rep = self.__lian_jia_session.get(self.__base_url + district.url)
        soup = BeautifulSoup(rep.text, 'lxml')

        xiao_qu_list = self.__parse_page(soup, district.id)
        page_url_list = utils.get_all_page(soup)
        for url in page_url_list:
            rep = self.__lian_jia_session.get(self.__base_url + url)
            soup = BeautifulSoup(rep.text, 'lxml')
            xiao_qu_list2 = self.__parse_page(soup, district.id)
            xiao_qu_list.extend(xiao_qu_list2)
        self.__logger.info('小区发现房源 ==> {0}'.format(len(xiao_qu_list)))
        return xiao_qu_list

    def __parse_page(self, soup, district):
        xiao_qu_list = []
        li_arr = soup.select('ul.listContent li')
        for li in li_arr:
            xiao_qu = XiaoQu()
            xiao_qu.district = district
            div = li.find('div', attrs={'class', 'info'})
            a = div.find('div', attrs={'class', 'title'}).a
            href = a['href']
            index = href.rfind('/', 0, len(href)-1)
            xiao_qu.url = href[index+1: len(href)-1]
            name = a.get_text(strip=True)
            if xiao_qu.url not in self.__xiao_qu_urls:
                self.__xiao_qu_urls.add(xiao_qu.url)
                xiao_qu.name = name
                house_info = div.find('div', attrs={'class', 'houseInfo'}).get_text(strip=True)
                xiao_qu.cheng_jiao_90 = int(house_info[house_info.find('交')+1:house_info.find('套')])
                xiao_qu.chu_zu = int(house_info[house_info.rfind('|') + 1:house_info.rfind('套')])
                nian_dai = div.select('div.positionInfo')[0].get_text(strip=True)
                nian_dai = nian_dai[nian_dai.find('/') + 1: nian_dai.find('年')]
                if nian_dai.isdigit():
                    xiao_qu.nian_dai = int(nian_dai)
                else:
                    xiao_qu.nian_dai = 0
                average_price = li.find('div', attrs={'class', 'totalPrice'}).get_text(strip=True)
                average_price = average_price[:average_price.find('元')]
                if average_price.isdigit():
                    xiao_qu.average_price = float(average_price)
                zai_shou = li.find('div', attrs={'class', 'xiaoquListItemSellCount'}).find('a').get_text(strip=True)
                xiao_qu.zai_shou = int(zai_shou[:zai_shou.find('套')])
                xiao_qu_list.append(xiao_qu)
        return xiao_qu_list


if __name__ == '__main__':
    filename, city = sys.argv
    AllXiaoQu(city).get_xiao_qu_list()
