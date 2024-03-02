import time

from faker import Faker
import random
import operate_mysql
import generate_customer_inf


def return_customer_login():
    fake = Faker()

    # 用户名称
    name = fake.name()
    # 用户密码
    password = fake.password(length=10, special_chars=True, digits=True, upper_case=True, lower_case=True)
    # 用户状态
    status_list = [1, 0]
    status = random.choice(status_list)

    customer_login = (name, password, status)

    return customer_login


if __name__ == "__main__":

    count = 0

    while True:
        # 写入customer_login
        sql = "insert into customer_login(login_name, password, user_stats) values (%s, %s, %s)"
        customer_login_params = return_customer_login()
        operate_mysql.insert(sql, customer_login_params)

        # 写入customer_inf
        # 获取 customer_login_max_id
        customer_id = operate_mysql.get_customer_login_max_id()
        customer_inf_params = generate_customer_inf.return_customer_inf()
        customer_inf_params_list = list(customer_inf_params)
        customer_inf_params_list.append(customer_id)
        customer_inf_params_tuple = tuple(customer_inf_params_list)
        sql = ("INSERT INTO mall.customer_inf ( customer_name, identity_card_type, identity_card_no, mobile_phone, "
               "customer_email, gender, user_point, register_time, birthday, customer_level, user_money, "
               "customer_id)VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        operate_mysql.insert(sql, customer_inf_params_tuple)

        count += 1
        print(f'已写入{count}条数据')

        time.sleep(1)
