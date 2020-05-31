package com.merkey.dao.goods;

import com.merkey.entity.goods.CityInfo;

import java.util.List;

/**
 * Description：城市信息dao层接口<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public interface ICityInfoDao {
    /**
     * 查询处所有的城市信息
     *
     * @return
     */
    List<CityInfo> findAll();
}
