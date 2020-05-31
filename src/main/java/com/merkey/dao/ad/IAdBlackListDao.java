package com.merkey.dao.ad;


import com.merkey.entity.ad.AdBlackList;

import java.util.List;

/**
 * Description：黑名单用户dao层接口<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public interface IAdBlackListDao {
    /**
     * 将黑名单用户批量落地到db中存储起来
     *
     * @param beans 既包含历史的黑名单用户信息，也包含新拉黑的用户信息
     */
    void insertBatchDealWith(List<AdBlackList> beans);

    /**
     * 查询所有黑名单用户信息
     *
     * @return
     */
    List<AdBlackList> findAll();
}
