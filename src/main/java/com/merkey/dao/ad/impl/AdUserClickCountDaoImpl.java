package com.merkey.dao.ad.impl;

import com.merkey.dao.ad.IAdUserClickCountDao;
import com.merkey.entity.ad.AdUserClickCount;
import com.merkey.utils.DBCPUitl;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.SQLException;
import java.util.List;

/**
 * Description：每天各用户对各广告的点击次数 dao层接口实现类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年10月07日
 *
 * @author merkey
 * @version : 1.0
 */
public class AdUserClickCountDaoImpl implements IAdUserClickCountDao {
    private QueryRunner queryRunner;

    public AdUserClickCountDaoImpl() {
        queryRunner = new QueryRunner(DBCPUitl.getDataSource());
    }

    @Override
    public void batchDealWith(List<AdUserClickCount> beans) {
        String sql = "insert into ad_user_click_count(`date`,user_id,ad_id,click_count) values(?,?,?,?) " +
                "on duplicate key update click_count=?";

        Object[][] batchParmas = new Object[beans.size()][];
        //需要修改数组中的元素值，必须使用普通for循环
        for (int i = 0; i < beans.size(); i++) {
            AdUserClickCount bean = beans.get(i);
            batchParmas[i] = new Object[]{bean.getDate(), bean.getUser_id(), bean.getAd_id(), bean.getClick_count(), bean.getClick_count()};
        }
        try {
            queryRunner.batch(sql, batchParmas);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("异常信息是：" + e.getMessage());
        }
    }
}
