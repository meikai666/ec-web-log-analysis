package com.merkey.entity.goods;

/**
 * Description：产品状态实体类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年10月04日
 *
 * @author merkey
 * @version : 1.0
 */
public class ProductStatus {
    /**
     * 产品状态信息 （{"product_status": 0}，0~>自营；1~>第三方
     */
    private int product_status;

    public int getProduct_status() {
        return product_status;
    }

    public void setProduct_status(int product_status) {
        this.product_status = product_status;
    }
}
