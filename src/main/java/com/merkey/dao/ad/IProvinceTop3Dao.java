package com.merkey.dao.ad;


import com.merkey.entity.ad.AdProvinceTop3;

import java.util.List;

/**
 * Description：统计每天各省份top3热门广告dao层接口<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年10月08日
 *
 * @author merkey
 * @version : 1.0
 */
public interface IProvinceTop3Dao {
    /**
     * 批处理 （批量新增和批量插入）
     *
     * @param beans
     */
    void batchDealWith(List<AdProvinceTop3> beans);
}
