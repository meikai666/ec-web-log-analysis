package com.merkey.dao.session;

import com.merkey.entity.session.SessionDetail;

import java.util.List;

/**
 * Description：用来存储随机抽取出来的session的明细数据、top10品类的session的明细数据的dao层接口<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public interface ISessionDetailDao {
    /**
     * 批量保存session明细数据bean
     *
     * @param beans
     */
    void insertBatch(List<SessionDetail> beans);

    /**
     * 查询明细表中已经存在的所有的SessionDetail信息
     *
     * @return
     */
    List<SessionDetail> queryAll();
}
