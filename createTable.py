from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, FLOAT, DATETIME, DATE, Boolean, ForeignKey #区分大小写
from sqlalchemy.sql import func
from session import LianJiaSession
import sys

# 生成orm基类
base = declarative_base()


class District(base):
    __tablename__ = 'district'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), unique=True)
    url = Column(String(128), unique=True)
    parent = Column(Integer, ForeignKey('district.id'))


class XiaoQu(base):
    __tablename__ = 'xiao_qu'
    id = Column(Integer, primary_key=True, autoincrement=True)
    district = Column(Integer, ForeignKey('district.id'))
    name = Column(String(64))
    average_price = Column(FLOAT(8))
    zai_shou = Column(Integer)
    cheng_jiao_90 = Column(Integer)
    chu_zu = Column(Integer)
    nian_dai = Column(Integer)
    url = Column(String(128), unique=True)
    status = Column(Boolean)


class House(base):
    __tablename__ = 'house'
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(128))
    xiao_qu = Column(Integer, ForeignKey('xiao_qu.id'))
    price = Column(FLOAT(8))
    area = Column(FLOAT(8))
    unit_price = Column(FLOAT(8))
    flood = Column(String(32))
    hu_xing = Column(String(32))
    zhuang_xiu = Column(String(64))
    chao_xiang = Column(String(16))
    star = Column(Integer)
    status = Column(Boolean)
    url = Column(String(128), unique=True)
    create_time = Column(DATETIME, server_default=func.now())


class HouseNot(base):
    __tablename__ = 'house_not'
    id = Column(Integer, primary_key=True, autoincrement=True)
    # 记录之前的id，一遍跟踪之前的价格变化
    ori_id = Column(Integer)
    title = Column(String(128))
    xiao_qu = Column(Integer, ForeignKey('xiao_qu.id'))
    price = Column(FLOAT(8))
    area = Column(FLOAT(8))
    unit_price = Column(FLOAT(8))
    flood = Column(String(32))
    hu_xing = Column(String(32))
    zhuang_xiu = Column(String(64))
    chao_xiang = Column(String(16))
    star = Column(Integer)
    url = Column(String(128))
    create_time = Column(DATETIME)
    end_time = Column(DATETIME, server_default=func.now())


class ChengJiao(base):
    __tablename__ = 'cheng_jiao'
    id = Column(Integer, primary_key=True, autoincrement=True)
    # 记录之前的id，一遍跟踪之前的价格变化
    ori_id = Column(Integer)
    price = Column(FLOAT(8))
    gua_pai_jia = Column(FLOAT(8))
    unit_price = Column(FLOAT(8))
    star = Column(Integer)
    url = Column(String(128))
    zhou_qi = Column(Integer)
    tiao_jia = Column(Integer)
    dai_kan = Column(Integer)
    liu_lan = Column(Integer)
    deal_date = Column(DATE)


class PriceChange(base):
    __tablename__ = 'price_change'
    id = Column(Integer, primary_key=True, autoincrement=True)
    house_id = Column(Integer, ForeignKey('house.id'))
    pre_price = Column(FLOAT(8))
    price = Column(FLOAT(8))
    change_time = Column(DATETIME)


class PriceChangeNot(base):
    __tablename__ = 'price_change_not'
    id = Column(Integer, primary_key=True, autoincrement=True)
    house_not_id = Column(Integer, ForeignKey('house_not.id'))
    pre_price = Column(FLOAT(8))
    price = Column(FLOAT(8))
    change_time = Column(DATETIME)
    create_time = Column(DATETIME, server_default=func.now())


def create_view(engine):
    conn = engine.raw_connection()
    cursor = conn.cursor()
    sql = 'CREATE VIEW price_change_com AS SELECT p.house_id AS house_id, p.pre_price AS pre_price,p.price AS price,( p.price - p.pre_price ) AS priceChange, Round( ( p.price - p.pre_price ) / p.pre_price * 100, 2 ) AS fudu,p.change_time AS change_time FROM price_change p'
    cursor.execute(sql)
    sql = 'CREATE VIEW district_area AS SELECT  d1.id id,  d2.NAME district,  d1.NAME area,  d1.url url FROM district d1,   district d2   WHERE   d2.id = d1.parent'
    cursor.execute(sql)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        filename, city = sys.argv
        lian_jia_session = LianJiaSession(city)
    else:
        lian_jia_session = LianJiaSession()

    engine = lian_jia_session.get_sql_engine()

    base.metadata.create_all(engine)  # 创建表结构
    create_view(engine)
