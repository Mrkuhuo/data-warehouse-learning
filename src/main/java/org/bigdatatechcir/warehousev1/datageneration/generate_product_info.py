from faker import Faker
import random
from datetime import datetime, timedelta
import time


def return_product_info(database_type):
    fake = Faker(locale='zh_CN')

    # 商品编码
    product_core = fake.pyint(min_value=0, max_value=9999999, step=1)

    # 商品名称
    product_name = fake.company_prefix()

    # 国条码
    bar_code = fake.credit_card_number(card_type=None)

    # 一级分类ID
    one_category_id_list = [1, 2, 3, 4, 5]
    one_category_id = random.choice(one_category_id_list)

    # 二级分类ID
    two_category_id_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    two_category_id = random.choice(two_category_id_list)

    # 三级分类ID
    three_category_id_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    three_category_id = random.choice(three_category_id_list)

    # 商品销售价格
    price = round(fake.random.uniform(0, 10000), 2)

    # 商品加权平均成本
    average_cost = round(fake.random.uniform(0, 10000), 2)

    # 上下架状态：0下架1上架
    publish_status_list = [0, 1]
    publish_status = random.choice(publish_status_list)

    # 审核状态：0未审核，1已审核
    audit_status_list = [0, 1]
    audit_status = random.choice(audit_status_list)

    # 商品重量
    weight = round(fake.random.uniform(0, 100), 2)

    # 商品长度
    length = round(fake.random.uniform(0, 100), 2)

    # 商品高度
    height = round(fake.random.uniform(0, 100), 2)

    # 商品宽度
    width = round(fake.random.uniform(0, 100), 2)

    # 商品颜色
    color_type_list = ['红', '黄', '蓝', '黑']
    color_type = random.choice(color_type_list)

    # 生产日期
    production_date = fake.date_time_between_dates(
        datetime_start=datetime(2016, 1, 1),
        datetime_end=datetime(2023, 7, 21),
        tzinfo=None,
    )

    # 商品有效期
    shelf_life = fake.pyint(min_value=0, max_value=999, step=1)

    # 商品描述
    descript = fake.paragraph(nb_sentences=3, variable_nb_sentences=True, ext_word_list=None)

    # 商品录入时间
    start = datetime(2022, 5, 23, 0, 0, 0)
    end = datetime(2023, 5, 23, 17, 30, 0)
    indate = start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())))

    if database_type == 'mysql':
        product_info = (product_core, product_name, bar_code, one_category_id, two_category_id, three_category_id, price, average_cost, publish_status, audit_status, weight, length, height, width, color_type, production_date, shelf_life, descript, indate)
        return product_info

    else:
        # 获取当前时间戳
        timestamp = time.time()
        # 将时间戳转换为整数
        id = int(timestamp)
        production_date = production_date.strftime("%Y-%m-%d %H:%M:%S")
        indate = indate.strftime("%Y-%m-%d %H:%M:%S")
        # 创建一个 datetime 对象
        now = datetime.now()
        # 转换为字符串
        str_now = now.strftime("%Y-%m-%d %H:%M:%S")
        product_info = (id, product_core, product_name, bar_code, one_category_id, two_category_id, three_category_id, price, average_cost, publish_status, audit_status, weight, length, height, width, color_type, production_date, shelf_life, descript, indate, str_now)
        return product_info
