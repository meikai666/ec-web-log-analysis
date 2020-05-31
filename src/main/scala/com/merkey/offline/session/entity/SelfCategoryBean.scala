package com.merkey.offline.session.entity

import scala.beans.BeanProperty

/**
  * Description：自定义品类实体类（比较规则定制类）<br/>
  * Copyright (c) ， 2019， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  *
  * @author merkey
  * @version : 1.0
  */
class SelfCategoryBean extends Ordered[SelfCategoryBean] with Serializable {
  /**
    * 注意：@BeanProperty用来生成getter/setter方法
    *
    * 品类的id
    */
  @BeanProperty var category_id: Int = 0

  /**
    * 点击总数
    */
  @BeanProperty var total_click_category_cnt: Int = 0

  /**
    * 下单总数
    */
  @BeanProperty var total_order_category_cnt: Int = 0

  /**
    * 支付总数
    */
  @BeanProperty var total_pay_category_cnt: Int = 0


  /**
    * 自定义辅助构造器
    *
    * @param category_id
    * @param total_click_category_cnt
    * @param total_order_category_cnt
    * @param total_pay_category_cnt
    */
  def this(category_id: Int, total_click_category_cnt: Int, total_order_category_cnt: Int, total_pay_category_cnt: Int) {
    this()
    this.category_id = category_id
    this.total_click_category_cnt = total_click_category_cnt
    this.total_order_category_cnt = total_order_category_cnt
    this.total_pay_category_cnt = total_pay_category_cnt
  }


  /**
    * 在下述方法中定制比较规则
    *
    * 降序：
    * 其他-this
    *
    * 升序：
    * this-其他
    *
    * @param that
    * @return ==0: 两个实例相同；>0: 当前实例较之于参数传过来的实例要大一些；<0:当前实例较之于参数传过来的实例要小一些
    */
  override def compare(that: SelfCategoryBean): Int = {
    //首先比较点击次数
    //若点击次数相同，则比较下单总数
    //若下单总数相同，则比较支付总数
    var ret = that.total_click_category_cnt - this.total_click_category_cnt

    if (ret == 0) {
      ret = that.total_order_category_cnt - this.total_order_category_cnt
      if (ret == 0) {
        ret = that.total_pay_category_cnt - this.total_pay_category_cnt
      }
    }

    ret
  }


  override def toString = s"SelfCategoryBean(category_id=$category_id, total_click_category_cnt=$total_click_category_cnt, total_order_category_cnt=$total_order_category_cnt, total_pay_category_cnt=$total_pay_category_cnt)"
}
