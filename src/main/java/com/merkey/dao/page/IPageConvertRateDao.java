package com.merkey.dao.page;


import com.merkey.entity.page.PageConvertRate;

/**
 * Description：页面单跳转化率dao层接口<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public interface IPageConvertRateDao {
    /**
     * 将参数指定的bean保存到db中
     *
     * @param bean
     */
    void insert(PageConvertRate bean);
}
