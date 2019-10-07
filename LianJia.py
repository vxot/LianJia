import queue
import time
import sys
from XiaoQuHouses import XiaoQuHouses, ParseXiaoQuPage
from createTable import House, XiaoQu
from session import LianJiaSession


class LianJia:

    def __init__(self, city):
        self.lian_jia_session = LianJiaSession(city)
        self.__yaml_data = self.lian_jia_session.get_prop()
        self.__house_list = []
        self.__logger = self.lian_jia_session.get_logger()
        self.sql_session = self.lian_jia_session.get_sql_session()

    def parse(self, is_breaking=False):
        if not is_breaking:
            self.__clear_breaking()
        self.__rest_status()
        xiao_qu_queue = self.__get_xiao_qu_map()
        xiao_qu_id_soup_queue = queue.Queue()
        producer_arr = []
        consumer_arr = []
        for i in range(self.__yaml_data['producer_num']):
            producer = XiaoQuHouses(self.lian_jia_session, xiao_qu_queue, xiao_qu_id_soup_queue)
            producer.setName('producer - ' + str(i))
            producer_arr.append(producer)
            producer.start()
        for i in range(self.__yaml_data['consumer_num']):
            consumer = ParseXiaoQuPage(self.lian_jia_session, xiao_qu_id_soup_queue)
            consumer.setName('consumer - ' + str(i))
            consumer.start()
            consumer_arr.append(consumer)

        for item in producer_arr:
            item.join()
        self.__logger.info('producer completed!.....................')
        for i in consumer_arr:
            xiao_qu_id_soup_queue.put((None, None))
            xiao_qu_id_soup_queue.put((None, None))
        for item in consumer_arr:
            item.join()
        self.__logger.info('consumer completed!.....................')

    def __clear_breaking(self):
        xiao_qus = self.sql_session.query(XiaoQu)
        for item in xiao_qus:
            item.status = False
        self.sql_session.commit()

    def __get_xiao_qu_map(self):
        xiao_qus = self.sql_session.query(XiaoQu).filter(XiaoQu.status == False).filter(XiaoQu.zai_shou >= self.__yaml_data['min_house']).all()
        xiao_qu_quenue = queue.Queue()
        for item in xiao_qus:
            xiao_qu_quenue.put(item)
        self.__logger.info('发现小区数量 : [{0}](小区房源数量至少{1})'.format(xiao_qu_quenue.qsize(), self.__yaml_data['min_house']))
        return xiao_qu_quenue

    def __rest_status(self):
        # 查询之前把status 全部致0
        t1 = time.time()
        houses = self.sql_session.query(House)
        houses.update({House.status: False})
        self.sql_session.commit()
        t2 = time.time()
        self.__logger.info('reset house status completed! use time : {0}'.format((t2 - t1)))


if '__main__' == __name__:
    filename, city = sys.argv
    # print(city)
    # city = 'shenzhen'
    lianJia = LianJia(city)
    lianJia.parse()
