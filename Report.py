import os
from datetime import datetime, timedelta, date
import matplotlib.pyplot as plt
import numpy as np
from session import LianJiaSession
import sys


class Report:

    def __init__(self, report_date, city=None):
        lj_session = LianJiaSession(city)
        self.city = lj_session.city
        engine = lj_session.get_sql_engine()
        self.__yaml_data = lj_session.get_prop()
        self.root_path = lj_session.get_log_path()
        self.logging = lj_session.get_logger()
        conn = engine.raw_connection()
        self.cursor = conn.cursor()
        self.query_time = report_date
        self.today = datetime.today()
        self.date_str = self.get_date_str()

    def report(self):
        argvs = dict()
        argvs['min_house'] = self.__yaml_data['min_house']
        argvs['query_date'] = self.query_time
        argvs['date'] = self.date_str

        total_house = self.total_house()
        argvs['total_house'] = total_house

        argvs['zhang_jia'] = self.get_zd_count(zd='z')
        argvs['die_jia'] = self.get_zd_count(zd='d')

        total_price_change = argvs['zhang_jia'] + argvs['die_jia']
        argvs['total_price_change'] = total_price_change
        if total_price_change == 0:
            argvs['die_jia_p'] = 0
            argvs['zhang_jia_p'] = 0
        else:
            argvs['die_jia_p'] = argvs['die_jia']/total_price_change*100
            argvs['zhang_jia_p'] = argvs['zhang_jia']/total_price_change*100
        argvs['price_change_p'] = total_price_change/total_house*100

        argvs['fu_du'] = 5
        argvs['fu_du_z'] = self.get_zd_count(zd='z', fu_du=argvs['fu_du'])

        argvs['fu_du_d'] = self.get_zd_count(zd='d', fu_du=argvs['fu_du'])
        argvs['fu_du_t'] = argvs['fu_du_z'] + argvs['fu_du_d']

        argvs['new_house_count'] = self.get_new_house_count()
        argvs['xiao_qu_count'] = self.get_xiao_qu_count()

        if argvs['fu_du_t'] == 0:
            argvs['die_jia_t_p'] = 0
            argvs['zhang_jia_t_p'] = 0
        else:
            argvs['die_jia_t_p'] = argvs['fu_du_d']/argvs['fu_du_t']*100
            argvs['zhang_jia_t_p'] = argvs['fu_du_z']/argvs['fu_du_t']*100

        district_arr = self.get_district()
        zt = 0
        dt = 0
        new_total = 0
        zhang_arr = []
        die_arr = []
        new_house_arr = []
        for item in district_arr:
            z = self.get_zd_count(zd='z', fu_du=argvs['fu_du'], district=item)
            d = self.get_zd_count(zd='d', fu_du=argvs['fu_du'], district=item)
            new_count = self.get_new_house_count(district=item)
            zt += z
            dt += d
            new_total += new_count
            new_house_arr.append(new_count)
            zhang_arr.append(z)
            die_arr.append(d)

        if argvs['new_house_count'] != new_total:
            raise Exception('总房源数计算错误！')
        # print('总计 {0} {1} {2}'.format(new_total, zt, dt))
        self.image(district_arr, zhang_arr, die_arr)
        self.get_new_house_pie(district_arr, new_house_arr)
        format_str = "武汉{query_date}({date})二手房大数据(来源于某家网)：\n" \
              "数据来源于{xiao_qu_count}个小区（在售房源大于等于{min_house}套），新增房源{new_house_count}套。\n" \
              "挂牌价格变动房源共计{total_price_change}套，" \
                     "其中涨价{zhang_jia}套（{zhang_jia_p:.2f}%），跌价{die_jia}套（{die_jia_p:.2f}%）。\n" \
              "涨跌幅5%以内的共计{fu_du_t}套，其中涨价{fu_du_z}({zhang_jia_t_p:.2f}%)套，跌价{fu_du_d}({die_jia_t_p:.2f}%)套。\n" \
              "\n以上信息仅供参考 : )"
        report_str = format_str.format(**argvs)
        self.logging.info(report_str)

    def get_xiao_qu_count(self):
        sql = 'select count(*) from xiao_qu xq where xq.zai_shou>=' + str(self.__yaml_data['min_house'])
        self.cursor.execute(sql)
        return self.cursor.fetchone()[0]

    def get_new_house_pie(self, ingredients, data):
        fig, ax = plt.subplots(figsize=(10, 8), subplot_kw=dict(aspect="equal"))

        def func(pct, allvals):
            absolute = int(pct / 100. * np.sum(allvals))
            return "{:.1f}%\n({:d}套)".format(pct, absolute)

        wedges, texts, autotexts = ax.pie(data, autopct=lambda pct: func(pct, data), textprops=dict(color="w"))

        ax.legend(wedges, ingredients,
                  title="区域",
                  loc="center left",
                  bbox_to_anchor=(1, 0, 0.5, 1)
                  )
        plt.setp(autotexts, size=8, weight="bold")
        ax.set_title("武汉二手房获取检测样本新增房源（{0}）".format(self.date_str))
        path = os.path.join(self.root_path, 'newHouse_{0}_{1}.png'.format(self.city, self.date_str))
        plt.savefig(path)

    def image(self, labels, zhang_arr, die_arr):
        x = np.arange(len(labels))  # the label locations
        width = 0.35  # the width of the bars

        fig, ax = plt.subplots()
        rects1 = ax.bar(x - width / 2, zhang_arr, width, label='涨价')
        rects2 = ax.bar(x + width / 2, die_arr, width, label='跌价')

        # Add some text for labels, title and custom x-axis tick labels, etc.
        ax.set_ylabel('房源数')
        ax.set_title('武汉二手房获取检测样本挂牌价涨跌幅5%以内（{0}）'.format(self.date_str))
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.legend()

        def autolabel(rects):
            """Attach a text label above each bar in *rects*, displaying its height."""
            for rect in rects:
                height = rect.get_height()
                ax.annotate('{}'.format(height),
                            xy=(rect.get_x() + rect.get_width() / 2, height),
                            xytext=(0, 3),  # 3 points vertical offset
                            textcoords="offset points",
                            ha='center', va='bottom')

        autolabel(rects1)
        autolabel(rects2)
        fig.tight_layout()
        path = os.path.join(self.root_path, 'priceChange5percent_{0}_{1}.png'.format(self.city, self.date_str))
        plt.savefig(path)

    def get_zd_count(self, zd=None, fu_du=None, district=None):
        where_sql = self.get_where(zd=zd, fu_du=fu_du, district=district)
        select_sql = 'SELECT count(*) FROM price_change_com p, house h, xiao_qu xq, district_area d '
        sql = '{0} WHERE {1}'.format(select_sql, where_sql)
        self.cursor.execute(sql)
        return self.cursor.fetchone()[0]

    def get_where(self, zd, fu_du=None, district=None):
        where_sql = 'p.house_id = h.id AND xq.id = h.xiao_qu AND xq.district = d.id '
        if '今日' == self.query_time:
            where_sql += ' AND TO_DAYS(p.change_time) = TO_DAYS(NOW()) '
        elif '最近' == self.query_time:
            where_sql += ' AND TO_DAYS(p.change_time) >= DATE_SUB(NOW(),INTERVAL  3 HOUR) '
        elif '昨日' == self.query_time:
            where_sql += ' AND TO_DAYS(p.change_time) = TO_DAYS(NOW())-1 '
        elif '本周' == self.query_time:
            where_sql += ' AND TO_DAYS(p.change_time) >= TO_DAYS(NOW())-6 '
        elif '上周' == self.query_time:
            where_sql += ' AND(date_format(p.change_time, "%Y-%m-%d")) = YEARWEEK(now())-1 '
        elif '上月' == self.query_time:
            where_sql += ' AND PERIOD_DIFF(date_format( now(),"%Y%m" ) , date_format(p.change_time,"%Y%m")) =1 '

        if zd == 'z':
            where_sql += ' AND p.priceChange>0 '
        elif zd == 'd':
            where_sql += ' AND p.priceChange<0 '
        if fu_du:
            where_sql += ' AND p.fudu<{0} and p.fudu>-{0} '.format(fu_du)
        if district:
            where_sql += ' AND d.district="{0}" '.format(district)
        return where_sql

    def get_date_str(self):
        date_str = self.today.strftime("%Y.%m.%d")
        if '昨日' == self.query_time:
            date_str = (self.today + timedelta(days=-1)).strftime("%Y.%m.%d")
        elif '本周' == self.query_time:
            date_str = (self.today + timedelta(days=-6)).strftime("%Y.%m.%d") + ' -- ' + date_str
        elif '上周' == self.query_time:
            year, week, day = self.today.isocalendar()
            date_str = (self.today + timedelta(days=-6 - day)).strftime("%Y.%m.%d") + ' -- ' + \
                       (self.today + timedelta(days=-day)).strftime("%Y.%m.%d")
        elif '上月' == self.query_time:
            date_str = '{0}月份'.format(self.today.month-1)
        return date_str

    def total_house(self):
        sql = 'select count(*) from house'
        self.cursor.execute(sql)
        total = self.cursor.fetchone()[0]
        return total

    def get_status_0(self):
        sql = 'SELECT * from house h WHERE h.status=0 '
        self.cursor.execute(sql)
        total = self.cursor.fetchone()[0]
        return total

    def get_district(self):
        sql = 'select name from district where parent is null'
        self.cursor.execute(sql)
        district_arr = []
        arr = self.cursor.fetchall()
        for item in arr:
            district_arr.append(item[0])
        return district_arr

    def get_new_house_count(self, district=None):
        if '今日' == self.query_time:
            where = ' WHERE TO_DAYS(h.create_time) = TO_DAYS(NOW()) '
        elif '最近' == self.query_time:
            where = ' WHERE TO_DAYS(h.create_time) >= DATE_SUB(NOW(),INTERVAL  3 HOUR) '
        elif '昨日' == self.query_time:
            where = ' WHERE TO_DAYS(h.create_time) >= TO_DAYS(NOW())-1 '
        elif '本周' == self.query_time:
            where = ' WHERE TO_DAYS(h.create_time) >= TO_DAYS(NOW())-6 '
        elif '上周' == self.query_time:
            where = ' WHERE YEARWEEK(date_format(h.create_time, "%Y-%m-%d")) = YEARWEEK(now())-1'
        elif '上月' == self.query_time:
            where = ' WHERE PERIOD_DIFF(date_format( now(), "%Y%m") , date_format(h.create_time, "%Y%m")) =1'
        if district is None:
            select = 'select count(*) from house h '
        else:
            select = 'select count(*) from house h , xiao_qu xq, district_area d '
            where += ' AND h.xiao_qu=xq.id AND xq.district=d.id AND d.district="{0}"'.format(district)
        self.cursor.execute(select + where)
        total = self.cursor.fetchone()[0]
        return total


if '__main__' == __name__:
    plt.rcParams['font.sans-serif'] = ['KaiTi']  # 指定默认字体
    plt.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号'-'显示为方块的问题
    # 今日  昨日  本周 上周 上月
    if len(sys.argv) == 3:
        filename, report_date, city = sys.argv
        report = Report(report_date, city)
    elif len(sys.argv) == 2:
        filename, report_date = sys.argv
        report = Report(report_date)
    elif len(sys.argv) == 1:
        report_date = '今日'
        report = Report(report_date)
    report.report()
