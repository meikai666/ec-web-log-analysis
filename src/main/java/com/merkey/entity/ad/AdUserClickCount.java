package com.merkey.entity.ad;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * Description：每天各用户对各广告的点击次数实体类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年10月07日
 *
 * @author merkey
 * @version : 1.0
 */
@Data
@NoArgsConstructor
public class AdUserClickCount {
    private String date;
    private int user_id;
    private int ad_id;
    private int click_count;

    //自定义相等规则，重写Object类中的equals和hashCode方法


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AdUserClickCount that = (AdUserClickCount) o;
        return user_id == that.user_id &&
                ad_id == that.ad_id &&
                Objects.equals(date, that.date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(date, user_id, ad_id);
    }


    public AdUserClickCount(String date, int user_id, int ad_id, int click_count) {
        this.date = date;
        this.user_id = user_id;
        this.ad_id = ad_id;
        this.click_count = click_count;
    }
}
