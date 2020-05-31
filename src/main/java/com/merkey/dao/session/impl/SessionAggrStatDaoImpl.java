package com.merkey.dao.session.impl;

import com.merkey.dao.session.ISessionAggrStatDao;
import com.merkey.entity.session.SessionAggrStat;
import com.merkey.utils.DBCPUitl;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.SQLException;

/**
 * Description：session聚合统计的结果数据访问层接口实现类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public class SessionAggrStatDaoImpl implements ISessionAggrStatDao {
    private QueryRunner queryRunner;

    public SessionAggrStatDaoImpl() {
        queryRunner = new QueryRunner(DBCPUitl.getDataSource());
    }

    @Override
    public void insert(SessionAggrStat bean) {
        try {
//            `task_id` int(11) NOT NULL,
//            `session_count` int(11) DEFAULT NULL,
//            `visit_len_1s_3s_ratio` double DEFAULT NULL,
//                    `visit_len_4s_6s_ratio` double DEFAULT NULL,
//                    `visit_len_7s_9s_ratio` double DEFAULT NULL,
//                    `visit_len_10s_30s_ratio` double DEFAULT NULL,
//                    `visit_len_30s_60s_ratio` double DEFAULT NULL,
//                    `visit_len_1m_3m_ratio` double DEFAULT NULL,
//                    `visit_len_3m_10m_ratio` double DEFAULT NULL,
//                    `visit_len_10m_30m_ratio` double DEFAULT NULL,
//                    `visit_len_30m_ratio` double DEFAULT NULL,
//                    `step_len_1_3_ratio` double DEFAULT NULL,
//                    `step_len_4_6_ratio` double DEFAULT NULL,
//                    `step_len_7_9_ratio` double DEFAULT NULL,
//                    `step_len_10_30_ratio` double DEFAULT NULL,
//                    `step_len_30_60_ratio` double DEFAULT NULL,
//                    `step_len_60_ratio` double DEFAULT NULL,
//


            queryRunner.update(
                    "insert into session_aggr_stat(task_id,session_count," +
                            "visit_len_1s_3s_ratio," +
                            "visit_len_4s_6s_ratio," +
                            "visit_len_7s_9s_ratio," +
                            "visit_len_10s_30s_ratio," +
                            "visit_len_30s_60s_ratio," +
                            "visit_len_1m_3m_ratio," +
                            "visit_len_3m_10m_ratio," +
                            "visit_len_10m_30m_ratio," +
                            "visit_len_30m_ratio," +
                            "step_len_1_3_ratio," +
                            "step_len_4_6_ratio," +
                            "step_len_7_9_ratio," +
                            "step_len_10_30_ratio," +
                            "step_len_30_60_ratio," +
                            "step_len_60_ratio) " +
                            "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    bean.getTask_id(),
                    bean.getSession_count(),
                    bean.getPeriod_1s_3s(),
                    bean.getPeriod_4s_6s(),
                    bean.getPeriod_7s_9s(),
                    bean.getPeriod_10s_30s(),
                    bean.getPeriod_30s_60s(),
                    bean.getPeriod_1m_3m(),
                    bean.getPeriod_3m_10m(),
                    bean.getPeriod_10m_30m(),
                    bean.getPeriod_30m(),
                    bean.getStep_1_3(),
                    bean.getStep_4_6(),
                    bean.getStep_7_9(),
                    bean.getStep_10_30(),
                    bean.getStep_30_60(),
                    bean.getStep_60());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
