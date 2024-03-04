from faker import Faker


def return_warehouse_shipping_info():
    fake = Faker(locale='zh_CN')

    # 物流公司名称
    ship_name = fake.company()

    # 物流公司联系人
    ship_contact = fake.name()

    # 物流公司联系电话
    telephone = fake.phone_number()

    # 配送价格
    price = round(fake.random.uniform(0, 10000), 2)

    warehouse_shipping_info = (ship_name, ship_contact, telephone, price)

    return warehouse_shipping_info


