from faker import Faker
import random


def return_product_brand_info():
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

    product_brand_info = (brand_name, telephone, brand_web, brand_logo, brand_desc, brand_status, brand_order)

    return product_brand_info
