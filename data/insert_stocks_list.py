# coding:utf-8

import pymysql
import csv

def connect():
    """
    连接mysql数据库
    :return:
    """
    conn = pymysql.connect(host="ubuntu2", port=3306,
                            user="hadoop", password="hadoop",
                            database="finance", charset="utf8")
    return conn
 
 
def insert_mysql(read_path, table_name, fields):
    """
    本地数据插入mysql
    :param read_path:  文件路径
    :return:
    """
    csv_file = open(read_path, "r", encoding="UTF-8")
    reader_csv = csv.reader(csv_file)
    conn = connect()
    cursor = conn.cursor()
    sql = "INSERT INTO `{table}` ({field}) VALUES ({val});".format(
        table=table_name,
        field='`' + '`,`'.join(fields) + '`',
        val=','.join(['%s'] * len(fields))
    )
    print(sql)
    value_list = []
    try:
        for line in reader_csv:
            temp_list = []
            for i in range(len(fields)):
                print(line[i])
                temp_list.append(line[i])
            # print(temp_list)
            value_list.append(temp_list)
            if len(value_list) == 100:
                cursor.executemany(sql, value_list)
                conn.commit()
                value_list = []
        if value_list:
            cursor.executemany(sql, value_list)
            conn.commit()
    except Exception as e:
        print("执行MySQL: %s 时出错：%s" % (sql, e))
    finally:
        cursor.close()
        conn.close()
        csv_file.close()
 
 
if __name__ == '__main__':
    read_path = "D:/VSCode/Quantitative-trading-of-stocks/data/AllStocks.csv"
    table_name = "stocks_list"
    fields = ["market", "stocks_code", "stocks_name", "listing_date"]
    insert_mysql(read_path, table_name, fields)




