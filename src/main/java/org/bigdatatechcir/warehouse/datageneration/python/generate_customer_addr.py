from faker import Faker
import random


def return_customer_addr():
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

    return customer_addr
