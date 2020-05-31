package com.merkey.dao.session;


import com.merkey.entity.session.Top10CategorySession;

import java.util.List;

/**
 * Description：top10每个品类的点击top10的session DAO层接口<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年10月03日
 *
 * @author merkey
 * @version : 1.0
 */
public interface ITop10CategorySessionDao {
    /**
     * 批量保存top10每个品类的点击top10的session
     *
     * @param beans
     */
    void insertBatch(List<Top10CategorySession> beans);
}
