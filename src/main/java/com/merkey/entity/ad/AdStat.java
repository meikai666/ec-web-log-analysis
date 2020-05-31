package com.merkey.entity.ad;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Description：用于封装每天各省各城市各广告的点击量的实体类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年10月08日
 *
 * @author merkey
 * @version : 1.0
 */
@Data
@NoArgsConstructor
public class AdStat {
    private String date;
    private String province;
    private String city;
    private int ad_id;
    private int click_count;

    public AdStat(String date, String province, String city, int ad_id, int click_count) {
        this.date = date;
        this.province = province;
        this.city = city;
        this.ad_id = ad_id;
        this.click_count = click_count;
    }
}
