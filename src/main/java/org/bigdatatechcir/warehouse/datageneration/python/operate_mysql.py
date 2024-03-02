import mysql.connector


def get_mysql_connect():
    cnx = mysql.connector.connect(user='root', password='',
                                  host='192.168.154.131',
                                  database='mall')
    return cnx


def insert(sql, params):
    global cursor, cnx
    try:
        # 创建连接
        cnx = get_mysql_connect()
        # 创建游标
        cursor = cnx.cursor()

        # 执行SQL语句
        cursor.execute(sql, params)

        # 提交到数据库
        cnx.commit()

        print(sql + " success !!!")

    except Exception as e:
        print('failed')
        print(e)
    finally:
        # 关闭游标 & 连接
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()


def get_customer_login_max_id():
    cnx = get_mysql_connect()
    # 创建游标
    cursor = cnx.cursor()

    # 执行SQL，并返回收影响行数
    cursor.execute("select max(customer_id) from customer_login")

    # 获取最大的id
    max_customer_id = cursor.fetchone()[0]

    return max_customer_id
