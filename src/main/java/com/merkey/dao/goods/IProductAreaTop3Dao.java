package com.merkey.dao.goods;

import com.merkey.entity.goods.ProductAreaTop3;

import java.util.List;

/**
 * Description：统计各个区域下top3产品之dao层接口<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年10月04日
 *
 * @author merkey
 * @version : 1.0
 */
public interface IProductAreaTop3Dao {
    /**
     * 批量保存各个区域下top3商品到db中
     *
     * @param beans
     */
    void insertBatch(List<ProductAreaTop3> beans);
}
