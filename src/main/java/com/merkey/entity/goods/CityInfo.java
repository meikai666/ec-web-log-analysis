package com.merkey.entity.goods;

import lombok.Data;

/**
 * Description：城市信息实体类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
@Data
public class CityInfo {
    /**
     * 编号
     */
    private int cityId;

    private String cityName;

    private String area;
}
