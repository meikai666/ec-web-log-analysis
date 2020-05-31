package com.merkey.dao.session.impl;

import com.merkey.dao.session.ITop10CategoryDao;
import com.merkey.entity.session.Top10Category;
import com.merkey.utils.DBCPUitl;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.SQLException;
import java.util.List;

/**
 * Description：操作点击、下单和支付数量排名前10的品类dao层接口实现类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年10月03日
 *
 * @author merkey
 * @version : 1.0
 */
public class Top10CategoryDaoImpl implements ITop10CategoryDao {
    private QueryRunner queryRunner;

    public Top10CategoryDaoImpl() {
        queryRunner = new QueryRunner(DBCPUitl.getDataSource());
    }

    @Override
    public void insertBatch(List<Top10Category> beans) {
        String sql = "insert into top10_category(task_id,category_id,click_count,order_count,pay_count) values(?,?,?,?,?)";

        Object[][] params = new Object[beans.size()][];
        for (int i = 0; i < params.length; i++) {
            Top10Category bean = beans.get(i);
            params[i] = new Object[]{bean.getTask_id(), bean.getCategory_id(), bean.getClick_count(), bean.getOrder_count(), bean.getPay_count()};
        }

        try {
            queryRunner.batch(sql, params);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("批量保存点击、下单和支付数量排名前10的品类信息发生异常了哦！异常信息是：" + e.getMessage());
        }
    }
}
