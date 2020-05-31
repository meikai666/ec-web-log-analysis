package com.merkey.dao.ad.impl;

import com.merkey.dao.ad.IProvinceTop3Dao;
import com.merkey.entity.ad.AdProvinceTop3;
import com.merkey.utils.DBCPUitl;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Description：每天各省各城市各广告的点击量的dao层接口实现类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public class AdProvinceTop3DaoImpl implements IProvinceTop3Dao {
    private QueryRunner queryRunner;

    public AdProvinceTop3DaoImpl() {
        queryRunner = new QueryRunner();
    }


    @Override
    public  void  batchDealWith(List<AdProvinceTop3> beans) {
        //获得连接的实例
        Connection conn = DBCPUitl.getConnection();

        try {
            //开启事务
            //DBCPUitl.beginTrasaction(conn);

            //执行业务操作
            //a)清空表数据 注意：若是真实的项目，体验度不好。
            //queryRunner.update(conn, "delete from ad_province_top3");

            //b)插入全新的数据 ~> 与内存中实时计算出来的【统计每天各省份top3热门广告】一致
            //存在就更新，不存在就插入
            StringBuffer sql = new StringBuffer("insert into ad_province_top3(`date`,province,ad_id,click_count) values(?,?,?,?)" +
                    " on duplicate key update click_count=?");

            Object[][] params = new Object[beans.size()][];

            for (int i = 0; i < params.length; i++) {
                AdProvinceTop3 bean = beans.get(i);
                params[i] = new Object[]{bean.getDate(), bean.getProvince(), bean.getAd_id(), bean.getClick_count(), bean.getClick_count()};
            }

            queryRunner.batch(conn, sql.toString(), params);

            //提交事务
           // DBCPUitl.commitTrasaction(conn);

        } catch (SQLException e) {
            e.printStackTrace();
            //回滚事务
            //DBCPUitl.rollbackTrasaction(conn);
        } finally {
            //资源释放
             DBCPUitl.releaseResource(conn);
        }
    }
}
