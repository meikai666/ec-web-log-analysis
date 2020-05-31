package com.merkey.dao.session.impl;

import com.merkey.dao.session.ISessionDetailDao;
import com.merkey.entity.session.SessionDetail;
import com.merkey.utils.DBCPUitl;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import java.sql.SQLException;
import java.util.List;

/**
 * Description：用来存储随机抽取出来的session的明细数据、top10品类的session的明细数据的dao层接口实现类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public class SessionDetailDaoImpl implements ISessionDetailDao {
    private QueryRunner queryRunner;

    public SessionDetailDaoImpl() {
        queryRunner = new QueryRunner(DBCPUitl.getDataSource());
    }

    @Override
    public void insertBatch(List<SessionDetail> beans) {
        String sql = "insert into session_detail(task_id,user_id,session_id,page_id,action_time,search_keyword,click_category_id,click_product_id,order_category_ids,order_product_ids,pay_category_ids,pay_product_ids) " +
                "values(?,?,?,?,?,?,?,?,?,?,?,?)";

        //二维数组用于存储参数
        //二维数组中每一个元素是一维数组，二维指的是SessionRandomExtract实例的个数，一维：实例中每个属性值，用于替换占位符
        Object[][] params = new Object[beans.size()][];
        for (int i = 0; i < params.length; i++) {
            SessionDetail bean = beans.get(i);
            params[i] = new Object[]{bean.getTask_id(), bean.getUser_id(), bean.getSession_id(), bean.getPage_id(),
                    bean.getAction_time(), bean.getSearch_keyword(), bean.getClick_category_id(),
                    bean.getClick_product_id(), bean.getOrder_category_ids(), bean.getOrder_product_ids(),
                    bean.getPay_category_ids(), bean.getPay_product_ids()};
        }

        try {
            queryRunner.batch(sql, params);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("批量保存数据到session的明细表中发生异常了哦！异常信息是：" + e.getMessage());
        }

    }

    @Override
    public List<SessionDetail> queryAll() {
        try {
            return queryRunner.query("select * from session_detail", new BeanListHandler<>(SessionDetail.class));
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("查询明细表中的数据发生异常了哦！异常信息是：" + e.getMessage());
        }
    }


}
