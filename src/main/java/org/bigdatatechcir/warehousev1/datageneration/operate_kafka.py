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
from json import JSONDecodeError

#
# 1. pip install confluent-kafka
# 2. 修改kafka链接信息
# conf = {'bootstrap.servers': "192.168.154.131:9092"}
# 3. 创建kafka topic
# 在 UI for Apache Kafka 中进行创建
# 4. 运行operate_kafka.py
#


# 写入kafka
from datetime import datetime, date

def send_to_kafka(topic, data_dict):
    try:
        # 配置kafka
        conf = {'bootstrap.servers': "192.168.244.129:9092"}  # 替换为你的kafka服务器地址

        producer = Producer(conf)

        # 将dict转换为json格式的字符串，设置ensure_ascii=False来保留非ASCII字符
        data_json = json.dumps(data_dict, ensure_ascii=False)

        # 将数据发送到kafka
        producer.produce(topic, data_json)

        print("发送 " + data_json + " success!!!")

        # 等待所有消息被发送
        producer.flush()

    except JSONDecodeError as e:
        print("JSONDecodeError:", e)
        raise e
    except Exception as e:
        print("Unexpected error:", e)
        raise e


def run():
    count = 0

    while True:

        # 获取用户登录ID信息
        # 获取最大 customer_login_id
        customer_login_max_id = operate_mysql.get_customer_login_max_id()
        if customer_login_max_id is None:
            customer_login_max_id = 0
        # 获取最小 customer_login_id
        customer_login_min_id = operate_mysql.get_customer_login_min_id()
        if customer_login_min_id is None:
            customer_login_min_id = 0
        # 获取任意 customer_login_id
        customer_login_random_id = random.randint(customer_login_min_id, customer_login_max_id)

        # 写入 kafka customer_login
        customer_login_key_tuple = ('login_name', 'password', 'user_stats','event_time', 'customer_id')
        customer_login_value_tuple = generate_customer_login.return_customer_login('kafka')
        customer_login_value_list = list(customer_login_value_tuple)
        customer_login_value_list.append(customer_login_random_id)
        customer_login_value_tuple = tuple(customer_login_value_list)
        customer_login_dict_val = {key: val for key, val in zip(customer_login_key_tuple, customer_login_value_tuple)}
        send_to_kafka("customer_login", customer_login_dict_val)

        # 写入 product_brand_info
        product_brand_info_tuple = generate_product_brand_info.return_product_brand_info('kafka')
        product_brand_info_key_tuple = ('brand_id', 'brand_name', 'telephone', 'brand_web', 'brand_logo', 'brand_desc', 'brand_status', 'brand_order', 'event_time')
        customer_addr_dict_val = {key: val for key, val in zip(product_brand_info_key_tuple, product_brand_info_tuple)}
        send_to_kafka("product_brand_info", customer_addr_dict_val)

        # 写入 product_supplier_info
        product_supplier_info_tuple = generate_product_supplier_info.return_product_supplier_info('kafka')
        product_supplier_info_key_tuple = ('supplier_id', 'supplier_code', 'supplier_name', 'supplier_type', 'link_man', 'phone_number', 'bank_name', 'bank_account', 'address', 'supplier_status', 'event_time')
        product_supplier_info_dict_val = {key: val for key, val in zip(product_supplier_info_key_tuple, product_supplier_info_tuple)}
        send_to_kafka("product_supplier_info", product_supplier_info_dict_val)

        # 写入 warehouse_shipping_info
        warehouse_shipping_info_tuple = generate_warehouse_shipping_info.return_warehouse_shipping_info('kafka')
        warehouse_shipping_info_key_tuple = ('ship_id', 'ship_name', 'ship_contact', 'telephone', 'price', 'event_time')
        warehouse_shipping_info_dict_val = {key: val for key, val in zip(warehouse_shipping_info_key_tuple, warehouse_shipping_info_tuple)}
        send_to_kafka("warehouse_shipping_info", warehouse_shipping_info_dict_val)


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
        if product_supplier_info_max_id is None:
            product_supplier_info_max_id = 0
        # 获取最小 product_brand_info_id
        product_supplier_info_min_id = operate_mysql.get_product_supplier_info_min_id()
        if product_supplier_info_min_id is None:
            product_supplier_info_min_id = 0
        # 获取任意 product_brand_info_id
        product_supplier_info_random_id = random.randint(product_supplier_info_min_id, product_supplier_info_max_id)

        # 获取快递公司ID
        warehouse_shipping_info_max_id = operate_mysql.get_warehouse_shipping_info_max_id()

        # 写入customer_inf
        customer_inf_params = generate_customer_inf.return_customer_inf("kafka")
        customer_inf_key_tuple = (
        'customer_inf_id', 'customer_name', 'identity_card_type', 'identity_card_no', 'mobile_phone', 'customer_email',
        'gender', 'user_point', 'register_time', 'birthday', 'customer_level', 'user_money', 'event_time', 'customer_id')
        customer_inf_params_list = list(customer_inf_params)
        customer_inf_params_list.append(customer_login_random_id)
        customer_inf_params_tuple = tuple(customer_inf_params_list)
        customer_inf_dict_val = {key: val for key, val in zip(customer_inf_key_tuple, customer_inf_params_tuple)}
        send_to_kafka("customer_inf", customer_inf_dict_val)

        # 写入 customer_addr
        customer_addr_params = generate_customer_addr.return_customer_addr("kafka")
        customer_addr_key_tuple = ('customer_addr_id', 'zip', 'province', 'city', 'district', 'address', 'is_default', 'event_time', 'customer_id')
        customer_addr_params_list = list(customer_addr_params)
        customer_addr_params_list.append(customer_login_random_id)
        customer_addr_params_tuple = tuple(customer_addr_params_list)
        customer_addr_dict_val = {key: val for key, val in zip(customer_addr_key_tuple, customer_addr_params_tuple)}
        send_to_kafka("customer_addr", customer_addr_dict_val)

        # 写入 customer_login_log
        customer_login_log_params = generate_customer_login_log.return_customer_login_log("kafka")
        customer_login_log_key_tuple = ('login_id', 'login_time', 'ipv4_public', 'login_type', 'event_time', 'customer_id')
        customer_login_log_params_list = list(customer_login_log_params)
        customer_login_log_params_list.append(customer_login_random_id)
        customer_login_log_params_tuple = tuple(customer_login_log_params_list)
        customer_login_log_dict_val = {key: val for key, val in zip(customer_login_log_key_tuple, customer_login_log_params_tuple)}
        send_to_kafka('customer_login_log', customer_login_log_dict_val)

        # 写入 product_info
        product_info_params = generate_product_info.return_product_info('kafka')
        product_info_key_tuple = ('product_id', 'product_core', 'product_name', 'bar_code', 'one_category_id', 'two_category_id', 'three_category_id', 'price', 'average_cost', 'publish_status', 'audit_status', 'weight', 'length', 'height', 'width', 'color_type', 'production_date', 'shelf_life', 'descript', 'indate', 'event_time', 'brand_id', 'supplier_id')
        product_info_params_list = list(product_info_params)
        product_info_params_list.append(product_brand_info_random_id)
        product_info_params_list.append(product_supplier_info_random_id)
        product_info_params_tuple = tuple(product_info_params_list)
        customer_login_log_dict_val = {key: val for key, val in zip(product_info_key_tuple, product_info_params_tuple)}
        send_to_kafka('product_info',customer_login_log_dict_val )

        # 获取产品ID信息
        # 获取最大 product_info_id
        product_info_max_id = operate_mysql.get_product_info_max_id()
        # 获取最小 product_info_id
        product_info_min_id = operate_mysql.get_product_info_min_id()
        # 获取任意 product_info_id
        product_info_random_id = random.randint(product_info_min_id, product_info_max_id)

        # 写入 order_master
        order_master_params = generate_order_master.return_order_master('kafka')
        order_master_key_tuple = ('order_id', 'order_sn', 'payment_method', 'order_money', 'district_money', 'shipping_money', 'payment_money', 'shipping_sn', 'create_time', 'shipping_time', 'pay_time', 'receive_time', 'order_status', 'order_point', 'event_time', 'customer_id', 'shipping_comp_name', 'product_id')
        order_master_params_list = list(order_master_params)
        order_master_params_list.append(customer_login_random_id)
        order_master_params_list.append(warehouse_shipping_info_max_id)
        order_master_params_list.append(product_info_random_id)
        order_master_params_tuple = tuple(order_master_params_list)
        order_master_dict_val = {key: val for key, val in zip(order_master_key_tuple, order_master_params_tuple)}
        send_to_kafka("order_master", order_master_dict_val)



        # 写入 order_cart
        order_cart_params = generate_order_cart.return_order_cart('kafka')
        order_cart_key_tuple = ('cart_id', 'product_amount', 'price', 'add_time', 'event_time', 'customer_id', 'product_id')
        order_cart_params_list = list(order_cart_params)
        order_cart_params_list.append(customer_login_random_id)
        order_cart_params_list.append(product_info_random_id)
        order_cart_params_tuple = tuple(order_cart_params_list)
        order_cart_dict_val = {key: val for key, val in zip(order_cart_key_tuple, order_cart_params_tuple)}
        send_to_kafka("order_cart", order_cart_dict_val)

        count += 1
        print(f'已写入{count}条数据')

        time.sleep(1)


if __name__ == "__main__":
    run()
