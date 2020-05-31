package com.merkey.dao.page.impl;

import com.merkey.dao.page.IPageConvertRateDao;
import com.merkey.entity.page.PageConvertRate;
import com.merkey.utils.DBCPUitl;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.SQLException;

/**
 * Description：xxx<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年10月03日
 *
 * @author merkey
 * @version : 1.0
 */
public class PageConvertRateDaoImpl implements IPageConvertRateDao {
    private QueryRunner queryRunner;

    public PageConvertRateDaoImpl() {
        queryRunner = new QueryRunner(DBCPUitl.getDataSource());
    }

    @Override
    public void insert(PageConvertRate bean) {
        String sql = "insert into t_page_convert_rate (task_id,convert_rate) values(?,?)";
        try {
            queryRunner.update(sql, bean.getTask_id(), bean.getConvert_rate());
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("保存页面单跳转化率失败了哦！异常信息是：" + e.getMessage());
        }
    }
}
