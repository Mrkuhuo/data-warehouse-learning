import operate_mysql

if __name__ == "__main__":

    sql = ("insert into customer_level_inf(customer_level, level_name, min_point, max_point) VALUES (1, '普通会员', 0, "
           "100),(2, '青铜', 101, 500),(3, '白银', 501, 2000),(4, '黄金', 2001, 5000 ),(5, '钻石', 5001, 10000000)")
    operate_mysql.insert(sql, '')