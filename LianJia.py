import queue
import sys
from XiaoQuHouses import XiaoQuHouses, ParseXiaoQuPage
from createTable import XiaoQu
from session import LianJiaSession
import utils


class LianJia:

    def __init__(self, city=None):
        self.lian_jia_session = LianJiaSession(city)
        self.__yaml_data = self.lian_jia_session.get_prop()
        self.__house_list = []
        self.__logger = self.lian_jia_session.get_logger()
        self.sql_session = self.lian_jia_session.get_sql_session()

    def parse(self, is_breaking=False):
        utils.reset_xiao_qu_status(self.sql_session, is_breaking)
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
            print(sys._getframe().f_lineno)
            if item.is_alive():
                print(sys._getframe().f_lineno)
                item.join()
        self.__logger.info('producer completed!.....................')
        print(sys._getframe().f_lineno)
        for i in consumer_arr:
            print(sys._getframe().f_lineno)
            xiao_qu_id_soup_queue.put((None, None))
            xiao_qu_id_soup_queue.put((None, None))
        for item in consumer_arr:
            print(sys._getframe().f_lineno)
            if item.is_alive():
                print(sys._getframe().f_lineno)
                item.join()
        print(sys._getframe().f_lineno)
        self.__logger.info('consumer completed!.....................')

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
        lianJia = LianJia(city)
    else:
        lianJia = LianJia()
    lianJia.parse(is_breaking=True)
