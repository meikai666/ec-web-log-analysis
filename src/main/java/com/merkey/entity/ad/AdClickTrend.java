package com.merkey.entity.ad;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Description：最近1小时各广告各分钟的点击量数据的封装<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
@Data
@NoArgsConstructor
public class AdClickTrend {
    /**
     * 日期 （每天）
     */
    private String date;

    /**
     * 广告编号
     */
    private int ad_id;

    /**
     * 分钟（20180328 14:50）
     */
    private String minute;

    /**
     * 点击次数
     */
    private int click_count;


    public AdClickTrend(String date, int ad_id, String minute, int click_count) {
        this.date = date;
        this.ad_id = ad_id;
        this.minute = minute;
        this.click_count = click_count;
    }

}
