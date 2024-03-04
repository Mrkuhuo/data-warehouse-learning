from faker import Faker
from datetime import datetime, timedelta
import random


def return_customer_login_log():

    fake = Faker(locale='zh_CN')

    # 用户登陆时间
    start = datetime(2022, 5, 23, 0, 0, 0)
    end = datetime(2023, 5, 23, 17, 30, 0)
    login_time = start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())))

    ipv4_public = fake.ipv4_public(network=False, address_class=None)

    # 用户状态
    login_type_list = [1, 0]
    login_type = random.choice(login_type_list)

    customer_login_log = (login_time, ipv4_public, login_type)

    return customer_login_log
