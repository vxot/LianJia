# 链家二手房挂牌价爬虫

**一. 环境准备**  
1. python3
2. 数据库Mysql
3. pip install -r requirements.txt

**二. 安装步骤**  
1. 创建数据库  
    CREATE DATABASE `数据库名` CHARACTER SET 'utf8' COLLATE 'utf8_bin';

2. 修改配置文件  
    修改properties.yml文件  
    ![Image text](https://raw.githubusercontent.com/vxot/LianJia/master/image/properties.png)  
    
3. 新建数据库表、视图  
    执行如下命令，新建数据库表  
    python createTable.py [shenzhen]（城市名，可选，如不填则从配置文件取默认城市，下同）  

4. 获取区域信息
    python AllDistrict.py [shenzhen]（城市名，可选）  
    ![Image text](https://raw.githubusercontent.com/vxot/LianJia/master/image/AllDistrict.jpg)  

5. 获取所有小区
    python AllXiaoQu.py [shenzhen]（城市名，可选）  
    ![Image text](https://raw.githubusercontent.com/vxot/LianJia/master/image/AllDistrict.jpg)  
   
6. 开始爬所有在售二手房
    python LianJia.py [shenzhen]（城市名，可选）  
    ![Image text](https://raw.githubusercontent.com/vxot/LianJia/master/image/AllHouse.jpg)  

**三. 执行方法**  
每天执行如下步骤：  
1. 爬数据  
    python LianJia.py [shenzhen]（城市名，可选）  
    ![Image text](https://raw.githubusercontent.com/vxot/LianJia/master/image/parseLog.jpg)  
 
2. 生成报表  
    python Report.py [今日]（日期，默认今日，可选今日，昨日，本周，上周, 上月） [shenzhen]（城市名，可选） 
    ![Image text](https://raw.githubusercontent.com/vxot/LianJia/master/image/newHouse.png)  
    ![Image text](https://raw.githubusercontent.com/vxot/LianJia/master/image/priceChange5percent.png)  

