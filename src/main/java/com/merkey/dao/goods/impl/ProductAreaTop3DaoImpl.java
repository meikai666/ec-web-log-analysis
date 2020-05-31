package com.merkey.dao.goods.impl;

import com.merkey.dao.goods.IProductAreaTop3Dao;
import com.merkey.entity.goods.ProductAreaTop3;
import com.merkey.utils.DBCPUitl;
import org.apache.commons.dbutils.QueryRunner;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;

/**
 * Description：统计各个区域下top3产品之dao层接口实现类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年10月04日
 *
 * @author merkey
 * @version : 1.0
 */
public class ProductAreaTop3DaoImpl implements IProductAreaTop3Dao, Serializable {
    private QueryRunner queryRunner;

    public ProductAreaTop3DaoImpl() {
        queryRunner = new QueryRunner(DBCPUitl.getDataSource());
    }

    @Override
    public void insertBatch(List<ProductAreaTop3> beans) {

        String sql = "insert into product_area_top3(task_id,area,area_level,product_id,product_name,click_count,product_status,city_names) values (?,?,?,?,?,?,?,?)";

        Object[][] params = new Object[beans.size()][];
        for (int i = 0; i < params.length; i++) {
            ProductAreaTop3 bean = beans.get(i);
            params[i] = new Object[]{bean.getTask_id(),
                    bean.getArea(), bean.getArea_level(), bean.getProduct_id(),
                    bean.getProduct_name(), bean.getClick_count(),
                    bean.getProduct_status(), bean.getCity_names()};
        }

        try {
            queryRunner.batch(sql, params);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("批量保存各个区域下top3产品信息失败了哦！异常信息是：" + e.getMessage());
        }

    }
}
