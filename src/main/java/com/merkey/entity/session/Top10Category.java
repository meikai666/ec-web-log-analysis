package com.merkey.entity.session;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Description：封装特定品类点击、下单和支付总数实体类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
@Data
@NoArgsConstructor
public class Top10Category {
    private int task_id;
    private int category_id;
    private int click_count;
    private int order_count;
    private int pay_count;

    public Top10Category(int task_id, int category_id, int click_count, int order_count, int pay_count) {
        this.task_id = task_id;
        this.category_id = category_id;
        this.click_count = click_count;
        this.order_count = order_count;
        this.pay_count = pay_count;
    }
}
