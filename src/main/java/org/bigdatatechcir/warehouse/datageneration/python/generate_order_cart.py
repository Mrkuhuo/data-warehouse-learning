from faker import Faker
from datetime import datetime, timedelta
import random


def return_order_cart():
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

    order_cart = (product_amount, price, add_time)

    return order_cart