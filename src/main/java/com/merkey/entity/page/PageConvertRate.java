package com.merkey.entity.page;

import lombok.Data;

/**
 * Description：页面单跳转化率实体类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
@Data
public class PageConvertRate {
    /**
     * task编号
     */
    private int task_id;

    /**
     * 转化率
     */
    private String convert_rate;

    public PageConvertRate(int task_id, String convert_rate) {
        this.task_id = task_id;
        this.convert_rate = convert_rate;
    }
}
