import time

from faker import Faker
import random
import operate_mysql
import generate_customer_inf
import generate_customer_addr
import generate_customer_login_log
from datetime import datetime, timedelta


def return_customer_login(database_type):
    fake = Faker()

    # 用户名称
    name = fake.name()
    # 用户密码
    password = fake.password(length=10, special_chars=True, digits=True, upper_case=True, lower_case=True)
    # 用户状态
    status_list = [1, 0]
    status = random.choice(status_list)

    if database_type == 'mysql':
        customer_login = (name, password, status)

        return customer_login
    else:
        # 创建一个 datetime 对象
        now = datetime.now()

        # 转换为字符串
        str_now = now.strftime("%Y-%m-%d %H:%M:%S")

        customer_login = (name, password, status, str_now)

        return customer_login
