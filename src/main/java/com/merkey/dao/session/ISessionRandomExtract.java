package com.merkey.dao.session;

import com.merkey.entity.session.SessionRandomExtract;

import java.util.List;

/**
 * Description：按时间比例随机抽取功能抽取出来的1000个session的dao层接口<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public interface ISessionRandomExtract {

    /**
     * 批量保存按时间比例随机抽取功能抽取出来的session信息
     *
     * @param beans
     */
    void insertBatch(List<SessionRandomExtract> beans);
}
