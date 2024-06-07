from faker import Faker
import time
from datetime import datetime, timedelta

def return_warehouse_shipping_info(database_type):
    fake = Faker(locale='zh_CN')

    # 物流公司名称
    ship_name = fake.company()

    # 物流公司联系人
    ship_contact = fake.name()

    # 物流公司联系电话
    telephone = fake.phone_number()

    # 配送价格
    price = round(fake.random.uniform(0, 10000), 2)

    if database_type == 'mysql':

        warehouse_shipping_info = (ship_name, ship_contact, telephone, price)

        return warehouse_shipping_info

    else:
        # 获取当前时间戳
        timestamp = time.time()
        # 将时间戳转换为整数
        id = int(timestamp)
        # 创建一个 datetime 对象
        now = datetime.now()
        # 转换为字符串
        str_now = now.strftime("%Y-%m-%d %H:%M:%S")
        warehouse_shipping_info = (id, ship_name, ship_contact, telephone, price, str_now)

        return warehouse_shipping_info

