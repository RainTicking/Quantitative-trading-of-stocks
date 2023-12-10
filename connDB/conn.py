# coding:utf-8

import pymysql
import datetime


class Conn(object):
    def __init__(self):
        # 获取当前日期时间对象
        current_time = datetime.datetime.now()
        new_time = current_time - datetime.timedelta(hours=8) #将当前时间减去8小时
        # 格式化日期为指定格式的字符串
        self.formatted_date = new_time.strftime("%Y-%m-%d")
        # mysql -h ubuntu2 -P3306  -uhadoop -phadoop --default-character-set=utf8
        try:
            self.conn = pymysql.connect(host="ubuntu2", port=3306,
                                user="hadoop", password="hadoop",
                                database="finance", charset="utf8")
            self.cursor = self.conn.cursor()
        except Exception as e:
            formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
            file_name = './logs/mysql_error_' + self.formatted_date + '.log'
            with open(file_name, "a") as file:  # ”w"代表着每次运行都覆盖内容
                file.write(formatted_time + ' connect mysql error: ' + str(e) + '\n')
                file.close()

    '''
      关闭mysql连接
    '''
    def close(self):
        self.conn.close()
        self.cursor.close()

    '''
      查询语句
    '''
    def select_mysql(self, table_name, fields, condition):
        """
        :param table_name:  表名
        :param fields:  字段列表
        :param condition:  查询条件
        """
        if condition != '':
            condition = ' where ' + condition
        else:
            condition = ''

        sql = "select {field} from `{table}`{condition};".format(
            table=table_name,
            field='`' + '`,`'.join(fields) + '`',
            condition=condition
        )
        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            # print("execute mysql: %s error:%s" %(sql,e))
            current_datetime = datetime.datetime.now()
            formatted_time = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
            file_name = './logs/mysql_error_' + self.formatted_date + '.log'
            with open(file_name, "a") as file:  # ”w"代表着每次运行都覆盖内容
                file.write(formatted_time + ' execute mysql: ' + sql + ' error: ' + str(e) + '\n')
                file.close()

    '''
      插入语句
    '''
    def insert_mysql(self, table_name, fields, vals, updatefields =[]):
        """
        :param table_name:  表名
        :param fields:  字段
        :param vals:  字段值
        :param updatefields:  更新字段
        :return:
        """
        if updatefields == []:
            updatestmt = ''
        else:
            updates = []
            for a,b in zip(* [updatefields] * 2):
                updates.append(a+'=VALUES('+b+")")
            updatestmt=' ON DUPLICATE KEY UPDATE '+','.join(updates)

        sql = "INSERT INTO `{table}` ({field}) VALUES ({val}){updatefield};".format(
            table=table_name,
            field='`' + '`,`'.join(fields) + '`',
            val=','.join(['%s'] * len(fields)),
            updatefield=updatestmt
        )
        value_list = []
        try:
            for val in vals:
                value_list.append(val)
                if len(value_list) == 1024:
                    self.cursor.executemany(sql, value_list)
                    self.conn.commit()
                    value_list = []
            if value_list:
                self.cursor.executemany(sql, value_list)
                self.conn.commit()
        except Exception as e:
            # print("执行MySQL: %s 时出错：%s" % (sql, e))
            current_datetime = datetime.datetime.now()
            formatted_time = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
            file_name = './logs/mysql_error_' + self.formatted_date + '.log'
            with open(file_name, "a") as file:  # ”w"代表着每次运行都覆盖内容
                file.write(formatted_time + ' execute mysql: ' + sql + ' error: ' + str(e) + '\n')
                file.close()

    '''
      清空数据
    '''
    def truncate_mysql(self,table_name):
        sql = "TRUNCATE `{table}`;".format(
            table=table_name
        )
        try:
            self.cursor.execute(sql)
        except Exception as e:
            # print("执行MySQL: %s 时出错：%s" % (sql, e))
            current_datetime = datetime.datetime.now()
            formatted_time = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
            file_name = './logs/mysql_error_' + self.formatted_date + '.log'
            with open(file_name, "a") as file:  # ”w"代表着每次运行都覆盖内容
                file.write(formatted_time + ' execute mysql: ' + sql + ' error: ' + str(e) + '\n')
                file.close()



