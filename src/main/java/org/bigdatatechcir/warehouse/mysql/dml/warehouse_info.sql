CREATE TABLE warehouse_info(
                               w_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '仓库ID',
                               warehouse_sn CHAR(5) NOT NULL COMMENT '仓库编码',
                               warehoust_name VARCHAR(10) NOT NULL COMMENT '仓库名称',
                               warehouse_phone VARCHAR(20) NOT NULL COMMENT '仓库电话',
                               contact VARCHAR(10) NOT NULL COMMENT '仓库联系人',
                               province SMALLINT NOT NULL COMMENT '省',
                               city SMALLINT NOT NULL COMMENT '市',
                               distrct SMALLINT NOT NULL COMMENT '区',
                               address VARCHAR(100) NOT NULL COMMENT '仓库地址',
                               warehouse_status TINYINT NOT NULL DEFAULT 1 COMMENT '仓库状态：0禁用，1启用',
                               modified_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后修改时间',
                               PRIMARY KEY pk_wid(w_id)
)ENGINE = innodb COMMENT '仓库信息表';