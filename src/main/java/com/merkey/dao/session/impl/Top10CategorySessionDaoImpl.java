package com.merkey.dao.session.impl;

import com.merkey.dao.session.ITop10CategorySessionDao;
import com.merkey.entity.session.Top10CategorySession;
import com.merkey.utils.DBCPUitl;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.SQLException;
import java.util.List;

/**
 * Description：top10每个品类的点击top10的session DAO层接口实现类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public class Top10CategorySessionDaoImpl implements ITop10CategorySessionDao {
    private QueryRunner queryRunner;

    public Top10CategorySessionDaoImpl() {
        queryRunner = new QueryRunner(DBCPUitl.getDataSource());
    }


    @Override
    public void insertBatch(List<Top10CategorySession> beans) {
        String sql = "insert into top10_category_session(task_id,category_id,session_id,click_count) values(?,?,?,?)";

        Object[][] params = new Object[beans.size()][];
        for (int i = 0; i < params.length; i++) {
            Top10CategorySession bean = beans.get(i);
            params[i] = new Object[]{bean.getTask_id(), bean.getCategory_id(), bean.getSession_id(), bean.getClick_count()};
        }

        try {
            queryRunner.batch(sql, params);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("批量保存top10每个品类的点击top10的session信息发生异常了哦！异常信息是：" + e.getMessage());
        }
    }
}
