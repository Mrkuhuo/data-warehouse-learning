import time

from faker import Faker
import random
import operate_mysql
import generate_customer_inf
import generate_customer_addr
import generate_customer_login_log


def return_customer_login(database_name):
    fake = Faker()

    # 用户名称
    name = fake.name()
    # 用户密码
    password = fake.password(length=10, special_chars=True, digits=True, upper_case=True, lower_case=True)
    # 用户状态
    status_list = [1, 0]
    status = random.choice(status_list)

    if "mysql" == database_name:
        customer_login = (name, password, status)

        return customer_login
    else:
        customer_login = [("name", name), ("password", password), ("status", status)]
        return customer_login

