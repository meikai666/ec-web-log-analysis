package com.merkey.common;

/**
 * Description：共通常量接口<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public interface CommonConstant {
    /**
     * 共通配置文件名
     */
    String COMMON_CONFIG_FILE_NAME = "config.properties";

    /**
     * DBCP连接池资源文件名
     */
    String DBCP_CONN_FILE_NAME = "dbcp-config.properties";

    /**
     * spark项目运行的模式
     */
    String SPARK_JOB_RUN_MODE = "spark.job.run.mode";


    /**
     * 共通初始化
     */
    String COMMON_INIT = "=0|";
    //累加器字符串中最后一个key对应的值
    String COMMON_INIT_2 = "=0";

    /**
     * 不同session数的标识 （去重之后的session总数）
     */
    String SESSION_COUNT = "session_count";

    /**
     * 时长标识
     */
    String TIME_PERIOD_1s_3s = "1s_3s";
    String TIME_PERIOD_4s_6s = "4s_6s";
    String TIME_PERIOD_7s_9s = "7s_9s";
    String TIME_PERIOD_10s_30s = "10s_30s";
    String TIME_PERIOD_30s_60s = "30s_60s";
    String TIME_PERIOD_1m_3m = "1m_3m";
    String TIME_PERIOD_3m_10m = "3m_10m";
    String TIME_PERIOD_10m_30m = "10m_30m";
    String TIME_PERIOD_30m = "30m";

    /**
     * 步长标识
     */
    String STEP_PERIOD_1_3 = "1_3";
    String STEP_PERIOD_4_6 = "4_6";
    String STEP_PERIOD_7_9 = "7_9";
    String STEP_PERIOD_10_30 = "10_30";
    String STEP_PERIOD_30_60 = "30_60";
    String STEP_PERIOD_60 = "60";

    /**
     * session聚合统计的结果常量
     * <p>
     * session_count=0|1s_3s=0|4s_6s=0|...|60=0
     * <p>
     * 设想：经过累加器不断操作后，值最终形如：
     * <p>
     * session_count=100|1s_3s=4|4s_6s=3|...|60=9
     */
    StringBuilder AGGR_RESULT = new StringBuilder()
            .append(SESSION_COUNT).append(COMMON_INIT)
            .append(TIME_PERIOD_1s_3s).append(COMMON_INIT)
            .append(TIME_PERIOD_4s_6s).append(COMMON_INIT)
            .append(TIME_PERIOD_7s_9s).append(COMMON_INIT)
            .append(TIME_PERIOD_10s_30s).append(COMMON_INIT)
            .append(TIME_PERIOD_30s_60s).append(COMMON_INIT)
            .append(TIME_PERIOD_1m_3m).append(COMMON_INIT)
            .append(TIME_PERIOD_3m_10m).append(COMMON_INIT)
            .append(TIME_PERIOD_10m_30m).append(COMMON_INIT)
            .append(TIME_PERIOD_30m).append(COMMON_INIT)
            .append(STEP_PERIOD_1_3).append(COMMON_INIT)
            .append(STEP_PERIOD_4_6).append(COMMON_INIT)
            .append(STEP_PERIOD_7_9).append(COMMON_INIT)
            .append(STEP_PERIOD_10_30).append(COMMON_INIT)
            .append(STEP_PERIOD_30_60).append(COMMON_INIT)
            .append(STEP_PERIOD_60).append(COMMON_INIT_2);

    /**
     * 样本数
     */
    String SAMPLE_CNT = "sample.cnt";

    /**
     * 黑名单判定标准：每天对某个广告点击超过100次的用户
     */
    String BLACK_LIST_CNT = "black.list.cnt";
}
