from faker import Faker
from datetime import datetime, timedelta
import random
import time


def return_customer_login_log(database_type):

    fake = Faker(locale='zh_CN')

    # 用户登陆时间
    start = datetime(2022, 5, 23, 0, 0, 0)
    end = datetime(2025, 5, 23, 17, 30, 0)
    login_time = start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())))

    ipv4_public = fake.ipv4_public(network=False, address_class=None)

    # 用户状态
    login_type_list = [1, 0]
    login_type = random.choice(login_type_list)

    if database_type == "mysql":

        customer_login_log = (login_time, ipv4_public, login_type)

        return customer_login_log

    else:
        # 获取当前时间戳
        timestamp = time.time()
        # 将时间戳转换为整数
        id = int(timestamp)

        login_time = login_time.strftime("%Y-%m-%d %H:%M:%S")

        # 创建一个 datetime 对象
        now = datetime.now()

        # 转换为字符串
        str_now = now.strftime("%Y-%m-%d %H:%M:%S")

        customer_login_log = (id, login_time, ipv4_public, login_type, str_now)
        return customer_login_log


