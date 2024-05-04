from faker import Faker
import random
import time
from datetime import datetime, timedelta


def return_product_brand_info(database_type):
    fake = Faker(locale='zh_CN')

    # 品牌名称
    brand_name = fake.company_prefix();

    # 联系电话
    telephone = fake.phone_number()

    # 品牌网络
    brand_web = fake.uri()

    # 品牌logo URL
    brand_logo = fake.image_url(width=None, height=None)

    # 品牌描述
    brand_desc = fake.paragraph(nb_sentences=3, variable_nb_sentences=True, ext_word_list=None)

    # 品牌状态,0禁用,1启用
    brand_status_list = [0, 1]
    brand_status = random.choice(brand_status_list)

    # 排序
    brand_order = 0

    if database_type == 'mysql':

        product_brand_info = (brand_name, telephone, brand_web, brand_logo, brand_desc, brand_status, brand_order)
        return product_brand_info

    else:
        # 获取当前时间戳
        timestamp = time.time()
        # 将时间戳转换为整数
        id = int(timestamp)
        # 创建一个 datetime 对象
        now = datetime.now()
        # 转换为字符串
        str_now = now.strftime("%Y-%m-%d %H:%M:%S")
        product_brand_info = (id, brand_name, telephone, brand_web, brand_logo, brand_desc, brand_status, brand_order, str_now)
        return product_brand_info


