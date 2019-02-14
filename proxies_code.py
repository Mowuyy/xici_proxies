import random
import time

import requests
import pymysql
import redis
import pickle
from lxml import etree
from fake_useragent import UserAgent


class ProxiesSpider(object):
    def __init__(self):
        self.user_agent_list = ["Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
                            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.62 Safari/537.36",
                            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1866.237 Safari/537.36",
                            "Mozilla/5.0 (X11; Linux i586; rv:31.0) Gecko/20100101 Firefox/31.0",
                            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.517 Safari/537.36",
                            "Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36",
                            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.116 Safari/537.36",
                            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
                            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.116 Safari/537.36",
                            "Mozilla/5.0 (iPad; U; CPU OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B334b Safari/531.21.10",
                            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36"]
        self.ua = UserAgent(verify_ssl=False)
        self.base_url = "https://www.xicidaili.com/nn/{}"
        self.url = [self.base_url.format(str(i)) for i in range(1, 2000)]
        self.conn_redis = redis.StrictRedis(host="127.0.0.1", port=6379, db=1)
        self.conn_mysql = pymysql.connect(host="127.0.0.1", port=3306, db="test", user="root", passwd="mysql", charset="utf8")
        self.cur = self.conn_mysql.cursor()
        self.ip_count = 1

    def get_ip(self):
        """获取ip"""
        proxies_str = self.conn_redis.rpop("proxies_info")
        return pickle.loads(proxies_str)

    def send_request(self, url, proxies_info):
        """发送请求"""
        time.sleep(random.uniform(0, 2))
        ip = proxies_info[0]
        port = proxies_info[1]
        protocol = proxies_info[2].lower()
        proxies = {protocol: "{}://{}:{}".format(protocol, ip, port)}
        print('[INFO]: send request: <{}>'.format(url))
        print('[INFO]: proxies info ==> {}://{}:{}'.format(protocol, ip, port))
        return requests.get(url, headers={"User-Agent": random.choice(self.user_agent_list)}, proxies=proxies)

    def create_table(self):
        """创建表"""
        set_pre = "set sql_notes = 0;"  # 消除表存在的警告
        table_sql = """create table if not exists info(
                        id int unsigned auto_increment primary key not null,
                        ip varchar(15) ,
                        port varchar(10),
                        address varchar(80),
                        status varchar(20),
                        protocol varchar(15),
                        speed varchar(50),
                        use_time varchar(50),
                        survival_time varchar(50),
                        verify_time varchar(50),
                        isdelete bit default 0
                        ) engine=innodb default charset=utf8;
                        """
        set_next = "set sql_notes = 1;"  # 消除表存在的警告
        try:
            self.cur.execute(set_pre)
            self.cur.execute(table_sql)
            self.cur.execute(set_next)
        except Exception as e:
            print(e)

    def parse_data(self, response):
        """解析数据"""
        html_obj = etree.HTML(response.content.decode('utf-8'))
        node_list = html_obj.xpath('//table[@id="ip_list"]//tr')[2:]
        for node in node_list:
            item = dict()
            item['ip'] = node.xpath('./td[2]/text()')[0].strip() if len(node.xpath('./td[2]/text()')) > 0 else None
            item['port'] = node.xpath('./td[3]/text()')[0].strip() if len(node.xpath('./td[3]/text()')) > 0 else None
            item['address'] = node.xpath('./td[4]/a/text()')[0].strip() if len(node.xpath('./td[4]/text()')) > 1 else None
            item['status'] = node.xpath('./td[5]/text()')[0].strip() if len(node.xpath('./td[5]/text()')) > 0 else None
            item['protocol'] = node.xpath('./td[6]/text()')[0].strip() if len(node.xpath('./td[6]/text()')) > 0 else None
            item['speed'] = node.xpath('./td[7]/div/@title')[0].strip() if len(node.xpath('./td[7]/div/@title')) > 0 else None
            item['use_time'] = node.xpath('./td[8]/div/@title')[0].strip() if len(node.xpath('./td[8]/div/@title')) > 0 else None
            item['survival_time'] = node.xpath('./td[9]/text()')[0].strip() if len(node.xpath('./td[9]/text()')) > 0 else None
            item['verify_time'] = node.xpath('./td[10]/text()')[0].strip() if len(node.xpath('./td[10]/text()')) > 0 else None
            print(item)
            sql = "insert into info(ip,port,address,status,protocol,speed,use_time,survival_time,verify_time) values(%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            try:
                self.cur.execute(sql, [item['ip'], item['port'], item['address'], item['status'], item['protocol'], item['speed'], item['use_time'], item['survival_time'], item['verify_time']])
                self.conn_mysql.commit()
            except Exception as e:
                print(e)
                self.conn_mysql.rollback()

    def main(self):
        """逻辑处理"""
        # 创建表信息
        self.create_table()
        # 1、获取代理信息
        proxies_info = self.get_ip()
        for url in self.url:
            if self.ip_count > 5:
                proxies_info = self.get_ip()
                self.ip_count = 1
            # 2、发送请求
            response = self.send_request(url, proxies_info)
            # 3、解析数据
            self.parse_data(response)
            self.ip_count += 1

    def __exit__(self):
        self.cur.close()
        self.conn_mysql.close()


if __name__ == '__main__':
    tool = ProxiesSpider()
    tool.main()
