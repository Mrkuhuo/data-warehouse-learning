from faker import Faker
import random
from datetime import datetime, timedelta

import operate_mysql


def return_customer_inf():
    fake = Faker(locale='zh_CN')

    # 用户正式姓名
    customer_name = fake.name()

    # 证件类型：1 身份证，2 军官证，3 护照
    identity_card_type_list = [1, 2, 3]
    identity_card_type = random.choice(identity_card_type_list)

    # 证件号码
    identity_card_no = fake.credit_card_number(card_type=None)

    # 手机号码
    mobile_phone = fake.phone_number()

    # 邮箱
    customer_email = fake.ascii_company_email()

    # 性别
    gender_list = ['男', '女']
    gender = random.choice(gender_list)

    # 用户积分
    user_point = fake.pyint(min_value=0, max_value=9999, step=1)

    # 注册时间
    start = datetime(2022, 5, 23, 0, 0, 0)
    end = datetime(2023, 5, 23, 17, 30, 0)
    register_time = start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())))

    # 会员生日
    birthday = fake.date_of_birth(tzinfo=None, minimum_age=0, maximum_age=115)

    # 会员级别：1 普通会员,2 青铜, 3 白银, 4 黄金, 5 钻石
    customer_level_list = [1, 2, 3, 4, 5]
    customer_level = random.choice(customer_level_list)

    # 用户余额
    user_money = fake.pyint(min_value=0, max_value=9999, step=1)

    customer_inf = (customer_name, identity_card_type, identity_card_no, mobile_phone, customer_email, gender, user_point,register_time, birthday, customer_level, user_money)

    return customer_inf

