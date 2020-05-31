package com.merkey.entity.ad;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Description：封装每天各省份top3热门广告实体类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年10月08日
 *
 * @author merkey
 * @version : 1.0
 */
@Data
@NoArgsConstructor
public class AdProvinceTop3 {
    private String date;
    private String province;
    private int ad_id;
    private int click_count;

    public AdProvinceTop3(String date, String province, int ad_id, int click_count) {
        this.date = date;
        this.province = province;
        this.ad_id = ad_id;
        this.click_count = click_count;
    }
}
