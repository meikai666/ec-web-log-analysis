package com.merkey.dao.ad.impl;

import com.merkey.dao.ad.IAdClickTrendDao;
import com.merkey.entity.ad.AdClickTrend;
import com.merkey.utils.DBCPUitl;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.SQLException;
import java.util.List;

/**
 * Description：对最近1小时滑动窗口内的数据，计算出各广告各分钟的点击量的dao层接口实现类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public class IAdClickTrendDaoImpl implements IAdClickTrendDao {
    private QueryRunner queryRunner;

    public IAdClickTrendDaoImpl() {
        queryRunner = new QueryRunner(DBCPUitl.getDataSource());
    }


    @Override
    public void batchDealWith(List<AdClickTrend> beans) {
        //存在就更新，不存在就插入
        StringBuffer sql = new StringBuffer("insert into ad_click_trend(`date`,ad_id,minute,click_count) values(?,?,?,?) ");
        sql.append("ON DUPLICATE KEY UPDATE click_count=?");

        Object[][] params = new Object[beans.size()][];

        for (int i = 0; i < params.length; i++) {
            AdClickTrend bean = beans.get(i);
            params[i] = new Object[]{bean.getDate(), bean.getAd_id(), bean.getMinute(), bean.getClick_count(), bean.getClick_count()};
        }
        try {
            queryRunner.batch(sql.toString(), params);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("批量保存或是更新【计算出各广告各分钟的点击量】发生异常了哦，异常信息是：" + e.getMessage());
        }

    }
}
