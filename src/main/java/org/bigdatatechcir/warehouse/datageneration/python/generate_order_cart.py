from faker import Faker
from datetime import datetime, timedelta
import random
from datetime import datetime, timedelta
import time

def return_order_cart(database_type):
    fake = Faker(locale='zh_CN')

    # 加入购物车商品数量
    product_amount = fake.pyint()

    # 商品价格
    price = fake.pyfloat()

    # 加入购物车时间
    start = datetime(2022, 5, 23, 0, 0, 0)
    end = datetime(2023, 5, 23, 17, 30, 0)
    add_time = start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())))

    if database_type == 'mysql':
        order_cart = (product_amount, price, add_time)
        return order_cart

    else:
        # 获取当前时间戳
        timestamp = time.time()
        # 将时间戳转换为整数
        id = int(timestamp)
        add_time = add_time.strftime("%Y-%m-%d %H:%M:%S")

        # 创建一个 datetime 对象
        now = datetime.now()

        # 转换为字符串
        str_now = now.strftime("%Y-%m-%d %H:%M:%S")

        order_cart = (id, product_amount, price, add_time, str_now)
        return order_cart