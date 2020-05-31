package com.merkey.dao.ad;

import com.merkey.entity.ad.AdUserClickCount;

import java.util.List;

/**
 * Description：每天各用户对各广告的点击次数 dao层接口<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年10月07日
 *
 * @author merkey
 * @version : 1.0
 */
public interface IAdUserClickCountDao {
    /**
     * 批处理 （批量新增和批量插入）
     * @param beans 既包含新的数据，也包含旧的表中已经存在的数据（date_user_id_ad_id，click_count一般不同）
     */
    void batchDealWith(List<AdUserClickCount> beans);
}
