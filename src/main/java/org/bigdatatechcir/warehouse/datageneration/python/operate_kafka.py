import generate_customer_inf
import generate_customer_login_log
import generate_customer_addr
import generate_customer_login
import random
import time
import operate_mysql

import generate_product_info
import generate_product_brand_info
import generate_product_supplier_info
import generate_warehouse_shipping_info
import generate_order_master
import generate_order_cart
import json


from confluent_kafka import Producer


# 写入kafka
def send_to_kafka(topic, data_dict):
    # 配置kafka
    conf = {'bootstrap.servers': "192.168.244.129:9092"}  # 替换为你的kafka服务器地址

    producer = Producer(conf)

    # 将tuple转换为json格式的字符串
    data_json = json.dumps(data_dict)

    # 将数据发送到kafka
    producer.produce(topic, data_json)

    print("发送 "+ data_json + " success!!!")

    # 等待所有消息被发送
    producer.flush()


def run():
    count = 0

    while True:
        # 写入customer_login
        customer_login_params_dict = generate_customer_login.return_customer_login("kafka")
        customer_login_params_dict_obj = {key: value for key, value in customer_login_params_dict}
        send_to_kafka("generate_customer_login", customer_login_params_dict_obj)

        # 写入 product_brand_info
        product_brand_info_tuple = generate_product_brand_info.return_product_brand_info()

        # 写入 product_supplier_info
        product_supplier_info_tuple = generate_product_supplier_info.return_product_supplier_info()

        # 写入 warehouse_shipping_info
        warehouse_shipping_info_tuple = generate_warehouse_shipping_info.return_warehouse_shipping_info()

        # 获取用户登录ID信息
        # 获取最大 customer_login_id
        customer_login_max_id = operate_mysql.get_customer_login_max_id()
        # 获取最小 customer_login_id
        customer_login_min_id = operate_mysql.get_customer_login_min_id()
        # 获取任意 customer_login_id
        customer_login_random_id = random.randint(customer_login_min_id, customer_login_max_id)

        # 获取品牌ID信息
        # 获取最大 product_brand_info_id
        product_brand_info_max_id = operate_mysql.get_product_brand_info_max_id()
        # 获取最小 product_brand_info_id
        product_brand_info_min_id = operate_mysql.get_product_brand_info_min_id()
        # 获取任意 product_brand_info_id
        product_brand_info_random_id = random.randint(product_brand_info_min_id, product_brand_info_max_id)

        # 获取供应商ID信息
        # 获取最大 product_brand_info_id
        product_supplier_info_max_id = operate_mysql.get_product_supplier_info_max_id()
        # 获取最小 product_brand_info_id
        product_supplier_info_min_id = operate_mysql.get_product_supplier_info_min_id()
        # 获取任意 product_brand_info_id
        product_supplier_info_random_id = random.randint(product_supplier_info_min_id, product_supplier_info_max_id)

        # 获取快递公司ID
        warehouse_shipping_info_max_id = operate_mysql.get_warehouse_shipping_info_max_id()

        # 写入customer_inf
        customer_inf_params = generate_customer_inf.return_customer_inf()
        customer_inf_params_list = list(customer_inf_params)
        customer_inf_params_list.append(customer_login_max_id)
        customer_inf_params_tuple = tuple(customer_inf_params_list)

        # 写入customer_addr
        customer_addr_params = generate_customer_addr.return_customer_addr()
        customer_addr_params_list = list(customer_addr_params)
        customer_addr_params_list.append(customer_login_max_id)
        customer_addr_params_tuple = tuple(customer_addr_params_list)

        # 写入 customer_login_log
        customer_login_log_params = generate_customer_login_log.return_customer_login_log()
        customer_login_log_params_list = list(customer_login_log_params)
        customer_login_log_params_list.append(customer_login_random_id)
        customer_login_log_params_tuple = tuple(customer_login_log_params_list)

        # 写入 product_info
        product_info_params = generate_product_info.return_product_info()
        product_info_params_list = list(product_info_params)
        product_info_params_list.append(product_brand_info_random_id)
        product_info_params_list.append(product_supplier_info_random_id)
        product_info_params_tuple = tuple(product_info_params_list)

        # 获取产品ID信息
        # 获取最大 product_info_id
        product_info_max_id = operate_mysql.get_product_info_max_id()
        # 获取最小 product_info_id
        product_info_min_id = operate_mysql.get_product_info_min_id()
        # 获取任意 product_info_id
        product_info_random_id = random.randint(product_info_min_id, product_info_max_id)

        # 写入order_master
        order_master_params = generate_order_master.return_order_master()
        order_master_params_list = list(order_master_params)
        order_master_params_list.append(customer_login_random_id)
        order_master_params_list.append(warehouse_shipping_info_max_id)
        order_master_params_tuple = tuple(order_master_params_list)

        # 写入order_cart
        order_cart_params = generate_order_cart.return_order_cart()
        order_cart_params_list = list(order_cart_params)
        order_cart_params_list.append(customer_login_random_id)
        order_cart_params_list.append(product_info_random_id)
        order_cart_params_tuple = tuple(order_cart_params_list)

        count += 1
        print(f'已写入{count}条数据')

        time.sleep(1)


if __name__ == "__main__":
    run()