package com.merkey.entity.goods;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Description：各个区域下top3热门产品实体类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年10月04日
 *
 * @author merkey
 * @version : 1.0
 */
@Data
@NoArgsConstructor
public class ProductAreaTop3 {
    private int task_id;
    private String area;
    private String area_level;
    private int product_id;
    private String product_name;
    private int click_count;
    private String product_status;
    private String city_names;

    public ProductAreaTop3(int task_id, String area, String area_level, int product_id, String product_name, int click_count, String product_status, String city_names) {
        this.task_id = task_id;
        this.area = area;
        this.area_level = area_level;
        this.product_id = product_id;
        this.product_name = product_name;
        this.click_count = click_count;
        this.product_status = product_status;
        this.city_names = city_names;
    }
}
