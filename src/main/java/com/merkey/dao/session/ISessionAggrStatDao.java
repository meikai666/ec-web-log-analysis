package com.merkey.dao.session;


import com.merkey.entity.session.SessionAggrStat;

/**
 * Description：session聚合统计的结果数据访问层接口<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public interface ISessionAggrStatDao {

    /**
     * 将session聚合统计的结果实例保存到db中
     *
     * @param bean
     */
    void insert(SessionAggrStat bean);
}
