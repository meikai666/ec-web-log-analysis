package com.merkey.dao.ad;

import com.merkey.entity.ad.AdStat;

import java.util.List;

/**
 * Description：每天各省各城市各广告的点击量的dao层接口<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public interface IAdStatDao {
    /**
     * 批处理 （批量新增和批量插入）
     * @param beans
     */
    void batchDealWith(List<AdStat> beans);
}
