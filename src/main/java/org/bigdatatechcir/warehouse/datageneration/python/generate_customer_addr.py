from faker import Faker
import random
import time
from datetime import datetime


def return_customer_addr(database_type):
    fake = Faker(locale='zh_CN')

    # 邮编
    postcode = fake.postcode()

    # 省份
    province = fake.province()

    # 城市
    city = fake.city()

    # 地区
    district = fake.district()

    # 具体地址
    address = fake.address()

    # 是够默认
    is_default_list = [1, 0]
    is_default = random.choice(is_default_list)

    customer_addr = (postcode, province, city, district, address, is_default)
    if database_type == 'mysql':
        return customer_addr
    else:
        # 获取当前时间戳
        timestamp = time.time()
        # 将时间戳转换为整数
        id = int(timestamp)

        # 创建一个 datetime 对象
        now = datetime.now()

        # 转换为字符串
        str_now = now.strftime("%Y-%m-%d %H:%M:%S")

        customer_addr = (id, postcode, province, city, district, address, is_default, str_now)
        return customer_addr

