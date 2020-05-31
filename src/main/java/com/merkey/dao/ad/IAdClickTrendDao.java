package com.merkey.dao.ad;

import com.merkey.entity.ad.AdClickTrend;

import java.util.List;

/**
 * Description：对最近1小时滑动窗口内的数据，计算出各广告各分钟的点击量的dao层接口<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public interface IAdClickTrendDao {
    /**
     * 批处理 （批量新增和批量插入）
     * @param beans
     */
    void batchDealWith(List<AdClickTrend> beans);
}
