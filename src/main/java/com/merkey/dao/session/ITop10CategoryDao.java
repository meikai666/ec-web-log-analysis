package com.merkey.dao.session;


import com.merkey.entity.session.Top10Category;

import java.util.List;

/**
 * Description：操作点击、下单和支付数量排名前10的品类dao层接口<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年10月03日
 *
 * @author merkey
 * @version : 1.0
 */
public interface ITop10CategoryDao {
    /**
     * 将指定的top10的品类信息落地到db中
     *
     * @param beans
     */
    void insertBatch(List<Top10Category> beans);
}
