import mysql.connector
import generate_customer_inf
import generate_customer_login_log
import generate_customer_addr
import generate_customer_login
import random
import time

import generate_product_info
import generate_product_brand_info
import generate_customer_level_inf
import generate_product_supplier_info


################################################# 读取数据库 ###############################################
# 获取数据库连接
def get_mysql_connect():
    cnx = mysql.connector.connect(user='root', password='',
                                  host='192.168.154.131',
                                  database='mall')
    return cnx


# 写入数据库
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


# 获取用户登录最大id
def get_customer_login_max_id():
    cnx = get_mysql_connect()
    # 创建游标
    cursor = cnx.cursor()

    # 执行SQL，并返回收影响行数
    cursor.execute("select max(customer_id) from customer_login")

    # 获取最大的id
    max_customer_id = cursor.fetchone()[0]

    return max_customer_id


# 获取用户登录最小id
def get_customer_login_min_id():
    cnx = get_mysql_connect()
    # 创建游标
    cursor = cnx.cursor()

    # 执行SQL，并返回收影响行数
    cursor.execute("select min(customer_id) from customer_login")

    # 获取最大的id
    min_customer_id = cursor.fetchone()[0]

    return min_customer_id


# 获取品牌信息最大id
def get_product_brand_info_max_id():
    cnx = get_mysql_connect()
    # 创建游标
    cursor = cnx.cursor()

    # 执行SQL，并返回收影响行数
    cursor.execute("select max(brand_id) from product_brand_info")

    # 获取最大的id
    product_brand_info_max_i = cursor.fetchone()[0]

    return product_brand_info_max_i


# 获取品牌信息最小id
def get_product_brand_info_min_id():
    cnx = get_mysql_connect()
    # 创建游标
    cursor = cnx.cursor()

    # 执行SQL，并返回收影响行数
    cursor.execute("select min(brand_id) from product_brand_info")

    # 获取最大的id
    product_brand_info_min_i = cursor.fetchone()[0]

    return product_brand_info_min_i


# 获取供应商最大id
def get_product_supplier_info_max_id():
    cnx = get_mysql_connect()
    # 创建游标
    cursor = cnx.cursor()

    # 执行SQL，并返回收影响行数
    cursor.execute("select max(supplier_id) from product_supplier_info")

    # 获取最大的id
    product_supplier_info_max_i = cursor.fetchone()[0]

    return product_supplier_info_max_i


# 获取供应商最小id
def get_product_supplier_info_min_id():
    cnx = get_mysql_connect()
    # 创建游标
    cursor = cnx.cursor()

    # 执行SQL，并返回收影响行数
    cursor.execute("select min(supplier_id) from product_supplier_info")

    # 获取最大的id
    product_supplier_info_min_i = cursor.fetchone()[0]

    return product_supplier_info_min_i


################################################ 写入数据库 #####################################################

if __name__ == "__main__":

    count = 0

    while True:
        # 写入customer_login
        customer_login_sql = "insert into customer_login(login_name, password, user_stats) values (%s, %s, %s)"
        customer_login_params = generate_customer_login.return_customer_login()
        insert(customer_login_sql, customer_login_params)

        # 获取用户登录ID信息
        # 获取最大 customer_login_id
        customer_login_max_id = get_customer_login_max_id()
        # 获取最小 customer_login_id
        customer_login_min_id = get_customer_login_min_id()
        # 获取任意 customer_login_id
        customer_login_random_id = random.randint(customer_login_min_id, customer_login_max_id)

        # 获取品牌ID信息
        # 获取最大 product_brand_info_id
        product_brand_info_max_id = get_product_brand_info_max_id()
        # 获取最小 product_brand_info_id
        product_brand_info_min_id = get_product_brand_info_min_id()
        # 获取任意 product_brand_info_id
        product_brand_info_random_id = random.randint(product_brand_info_min_id, product_brand_info_max_id)

        # 获取供应商ID信息
        # 获取最大 product_brand_info_id
        product_supplier_info_max_id = get_product_supplier_info_max_id()
        # 获取最小 product_brand_info_id
        product_supplier_info_min_id = get_product_supplier_info_min_id()
        # 获取任意 product_brand_info_id
        product_supplier_info_random_id = random.randint(product_supplier_info_min_id, product_supplier_info_max_id)

        # 写入customer_inf
        customer_inf_params = generate_customer_inf.return_customer_inf()
        customer_inf_params_list = list(customer_inf_params)
        customer_inf_params_list.append(customer_login_max_id)
        customer_inf_params_tuple = tuple(customer_inf_params_list)
        customer_inf_sql = ("INSERT INTO mall.customer_inf ( customer_name, identity_card_type, identity_card_no, "
                            "mobile_phone, customer_email, gender, user_point, register_time, birthday, "
                            "customer_level, user_money, customer_id)VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                            "%s, %s)")
        insert(customer_inf_sql, customer_inf_params_tuple)

        # 写入customer_addr
        customer_addr_params = generate_customer_addr.return_customer_addr()
        customer_addr_params_list = list(customer_addr_params)
        customer_addr_params_list.append(customer_login_max_id)
        customer_addr_params_tuple = tuple(customer_addr_params_list)
        customer_addr_sql = ("insert into mall.customer_addr(zip, province, city, district, address, is_default, "
                             "customer_id) values(%s, %s, %s, %s, %s, %s,%s)")
        insert(customer_addr_sql, customer_addr_params_tuple)

        # 写入 customer_login_log
        customer_login_log_params = generate_customer_login_log.return_customer_login_log()
        customer_login_log_params_list = list(customer_login_log_params)
        customer_login_log_params_list.append(customer_login_random_id)
        customer_login_log_params_tuple = tuple(customer_login_log_params_list)
        customer_login_log_sql = ("insert into mall.customer_login_log(login_time, login_ip, login_type, customer_id) "
                                  "values (%s, %s, %s, %s)")
        insert(customer_login_log_sql, customer_login_log_params_tuple)

        # 写入 product_brand_info
        product_brand_info_tuple = generate_product_brand_info.return_product_brand_info()
        product_brand_info_sql = ("insert into product_brand_info(brand_name, telephone, brand_web, brand_logo, "
                                  "brand_desc, brand_status, brand_order) values (%s, %s, %s, %s, %s, %s, %s)")
        insert(product_brand_info_sql, product_brand_info_tuple)

        # 写入 product_supplier_info
        product_supplier_info_tuple = generate_product_supplier_info.return_product_supplier_info()
        product_supplier_info_sql = ("insert into product_supplier_info(supplier_code, supplier_name, supplier_type, "
                                     "link_man, phone_number, bank_name, bank_account, address, supplier_status) "
                                     "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
        insert(product_supplier_info_sql, product_supplier_info_tuple)



        count += 1
        print(f'已写入{count}条数据')

        time.sleep(1)
