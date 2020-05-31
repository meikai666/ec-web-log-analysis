package com.merkey.dao.common.impl;

import com.merkey.dao.common.ITask;
import com.merkey.entity.common.Task;
import com.merkey.utils.DBCPUitl;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;

import java.sql.SQLException;

/**
 * Description：xxxx<br/>
 * Copyright (c) ，2019 ， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date： 2019年12月25日
 *
 * @author merkey
 * @version : 1.0
 */
public class ITaskDaoImpl implements ITask {
    private QueryRunner queryRunner = new QueryRunner(DBCPUitl.getDataSource());


    @Override
    public Task findTaskById(int task_id) {
        String sql = "select * from task where task_id=?";
        try {
            return queryRunner.query(sql, new BeanHandler<>(Task.class), task_id);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("根据任务编号查询任务详情失败了哦！异常信息是：" + e.getMessage());
        }
    }
}
