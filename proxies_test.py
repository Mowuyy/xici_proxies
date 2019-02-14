import requests
import pymysql
import redis
import pickle


class ProxiesPool(object):
    def __init__(self):
        # 链接mysql
        self.conn_mysql = pymysql.connect(host="127.0.0.1", port=3306, user="root", passwd="mysql", db="proxies", charset="utf8")
        self.cur = self.conn_mysql.cursor()
        # 链接redis
        self.conn_redis = redis.StrictRedis(host="127.0.0.1", port=6379, db=1)
        self.count = 1

    def get_ip(self):
        """获取ip、port信息"""
        sql = """select ip,port,protocol from info where speed<'0.10秒' and protocol='https';"""
        try:
            self.cur.execute(sql)
            return self.cur.fetchall()
        except Exception as e:
            print(e)

    def send_request(self, result):
        """发送百度请求"""
        ip = result[0]
        port = result[1]
        protocol = result[2]
        proxies = {protocol: "{}://{}:{}".format(protocol, ip, port)}
        return requests.get("https://www.baidu.com", proxies=proxies, timeout=3), result

    def parse_response(self, response, result):
        """判断请求状态"""
        if 200 == response.status_code:
            print(result)
            print("计数: {}".format(self.count))
            return pickle.dumps(result)
        else:
            print("失败信息: {}".format(result))

    def save_redis(self, result):
        """保存数据至redis"""
        self.conn_redis.lpush("proxies_info", result)

    def __exit__(self):
        self.cur.close()
        self.conn_mysql.close()

    def main(self):
        """逻辑处理"""
        # 1、从mysql获取代理信息
        result_tuple = self.get_ip()
        for result in result_tuple:
            # 1、发送百度请求
            response, result = self.send_request(result)
            # 2、验证请求是否成功
            result_dumps = self.parse_response(response, result)
            # 3、将代理信息存储至redis
            self.save_redis(result_dumps)
            self.count += 1


if __name__ == "__main__":
    tool = ProxiesPool()
    tool.main()

