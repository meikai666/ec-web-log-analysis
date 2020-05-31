package com.merkey.dao.session.impl;

import com.merkey.dao.session.ISessionRandomExtract;
import com.merkey.entity.session.SessionRandomExtract;
import com.merkey.utils.DBCPUitl;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.SQLException;
import java.util.List;

/**
 * Description：按时间比例随机抽取功能抽取出来的1000个session的dao层接口实现类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public class SessionRandomExtractImpl implements ISessionRandomExtract {
    private QueryRunner queryRunner;

    public SessionRandomExtractImpl() {
        queryRunner = new QueryRunner(DBCPUitl.getDataSource());
    }

    @Override
    public void insertBatch(List<SessionRandomExtract> beans) {
        String sql = "insert into session_random_extract(task_id,session_id,start_time,end_time,search_keywords,click_category_ids) values(?,?,?,?,?,?)";

        //二维数组用于存储参数
        //二维数组中每一个元素是一维数组，二维指的是SessionRandomExtract实例的个数，一维：实例中每个属性值，用于替换占位符
        Object[][] params = new Object[beans.size()][];
        for (int i = 0; i < params.length; i++) {
            SessionRandomExtract bean = beans.get(i);
            params[i] = new Object[]{bean.getTask_id(), bean.getSession_id(), bean.getStart_time(), bean.getEnd_time(), bean.getSearch_keywords(), bean.getClick_category_ids()};
        }

        try {
            queryRunner.batch(sql, params);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("批量保存按时间比例随机抽取功能抽取出来的session信息发生异常了哦！异常信息是：" + e.getMessage());
        }

    }
}
