-- 创建数据库
CREATE DATABASE app_db;

USE app_db;

-- 创建 orders 表
CREATE TABLE `orders` (
                          `id` INT NOT NULL,
                          `price` DECIMAL(10,2) NOT NULL,
                          PRIMARY KEY (`id`)
);

-- 插入数据
INSERT INTO `orders` (`id`, `price`) VALUES (1, 4.00);
INSERT INTO `orders` (`id`, `price`) VALUES (2, 100.00);

-- 创建 shipments 表
CREATE TABLE `shipments` (
                             `id` INT NOT NULL,
                             `city` VARCHAR(255) NOT NULL,
                             PRIMARY KEY (`id`)
);

-- 插入数据
INSERT INTO `shipments` (`id`, `city`) VALUES (1, 'beijing');
INSERT INTO `shipments` (`id`, `city`) VALUES (2, 'xian');

-- 创建 products 表
CREATE TABLE `products` (
                            `id` INT NOT NULL,
                            `product` VARCHAR(255) NOT NULL,
                            PRIMARY KEY (`id`)
);

-- 插入数据
INSERT INTO `products` (`id`, `product`) VALUES (1, 'Beer');
INSERT INTO `products` (`id`, `product`) VALUES (2, 'Cap');
INSERT INTO `products` (`id`, `product`) VALUES (3, 'Peanut');