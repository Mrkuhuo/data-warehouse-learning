from faker import Faker
import random


def return_product_supplier_info():
    fake = Faker(locale='zh_CN')

    # 供应商编码
    supplier_code = fake.credit_card_number(card_type=None)

    # 供应商名称
    supplier_name = fake.company()

    # 供应商类型：1.自营，2.平台
    supplier_type_list = [1, 2]
    supplier_type = random.choice(supplier_type_list)

    # 供应商联系人
    link_man = fake.name()

    # 联系电话
    phone_number = fake.phone_number()

    # 供应商开户银行名称
    bank_name = fake.company_prefix()

    # 银行账号
    bank_account = fake.bban()

    # 供应商地址
    address = fake.address()

    # 状态：0禁止，1启用
    supplier_status_list = [0, 1]
    supplier_status = random.choice(supplier_status_list)

    product_supplier_info = (supplier_code, supplier_name, supplier_type, link_man, phone_number, bank_name, bank_account, address, supplier_status)

    return product_supplier_info