# coding:utf-8

import pymysql

class Conn(object):
    def __init__(self):
        # mysql -h172.16.3.159 -P3307  -uadmin -pQR9I4Xtb7e6AYk6O --default-character-set=utf8
        self.conn = pymysql.connect(host="172.16.3.159", port=3307,
                            user="admin", password="QR9I4Xtb7e6AYk6O",
                            database="flying_fish", charset="utf8")
        self.cursor = self.conn.cursor()
    # 关闭mysql连接
    def close(self):
        self.conn.close()
        self.cursor.close()
    # 查询语句
    def select_mysql(self,sql):
        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("execute mysql: %s error:%s" %(sql,e))
    # 插入语句
    def insert_mysql(self,table_name, fields, vals, updatefields):
        sql = "INSERT INTO `{table}` ({field}) VALUES ({val}) ON DUPLICATE KEY UPDATE {updatefield};".format(
            table=table_name,
            field='`' + '`,`'.join(fields) + '`',
            val=','.join(['%s'] * len(fields)),
            updatefield=updatefields
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
            print("执行MySQL: %s 时出错：%s" % (sql, e))
    # 清空数据
    def truncate_mysql(self,table_name):
        sql = "TRUNCATE `{table}`;".format(
            table=table_name
        )
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print("执行MySQL: %s 时出错：%s" % (sql, e))
if __name__ == '__main__':
    conn = Conn()
    condition = "2021-02-25"
    sql = "SELECT `task_id`, `task_name`, `version_id`, `instance_id`, `app_id`, `cmd_conf` FROM `opt_task_info` where date(`ctime`) = '{cond}';".format(
            cond=condition
    )
    select_res = conn.select_mysql(sql)
    # 插入数据
    vals = []
    for row in select_res:
        task_id = row[0]
        task_name = row[1]
        version_id = row[2]
        instance_id = row[3]
        app_id = row[4]
        skew_loc = '333'
        temp = [task_id,task_name,version_id,instance_id,app_id,skew_loc]
        vals.append(temp)
    table_name = "skew_info"
    fields = ["task_id", "task_name", "version_id", "instance_id", "app_id","skew_loc"]
    updatefields = ["skew_list","skew_loc","ast_json"]
    conn.truncate_mysql(table_name)
    conn.insert_mysql(table_name, fields, vals, updatefields)
    conn.close()



