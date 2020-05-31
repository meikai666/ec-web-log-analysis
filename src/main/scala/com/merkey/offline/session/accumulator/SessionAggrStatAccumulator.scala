package com.merkey.offline.session.accumulator

import com.merkey.common.CommonConstant
import com.merkey.utils.StringUtils
import org.apache.spark.util.AccumulatorV2

/**
  * Description：自定义的累加器，用于记录session数据中各个统计维度出现的session的数量
  * 比如session时长：
  * 1s_3s=5
  * 4s_6s=8
  *
  * 比如session步长：
  * 1_3=5
  * 4_6=10
  *
  * 这种自定义的方式在工作过程中非常有用，可以避免写大量的tranformation操作
  * 在scala中自定义累加器，需要继承AccumulatorV2（spark旧版本需要继承AccumulatorParam）这样一个抽象类
  *
  * <br/>
  * Copyright (c) ， 2019， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  *
  * @author merkey
  * @version : 1.0
  */
class SessionAggrStatAccumulator extends AccumulatorV2[String, String] {
  //result： 记录的是当前累加器的最终结果
  //session_count=0|1s_3s=0|4s_6s...|60=0
  var result = CommonConstant.AGGR_RESULT.toString // 初始值

  /**
    * Returns if this accumulator is zero value or not. e.g. for a counter accumulator, 0 is zero value; for a list accumulator, Nil is zero value.
    * 推导出：累加器实例中封装的值若是等于初始值时，即为true;否则，为false
    *
    * @return
    */
  override def isZero: Boolean = CommonConstant.AGGR_RESULT.toString.equals(result)


  /**
    * 将一个Executor进程中的线程中的累加器的实例拷贝到别的线程中去
    *
    * 拷贝一个新的AccumulatorV2
    *
    * @return
    */
  override def copy(): AccumulatorV2[String, String] = {
    val myAccumulator = new SessionAggrStatAccumulator()
    //myAccumulator.result = this.result
    myAccumulator
  }

  /**
    * reset: 重置AccumulatorV2中的数据
    */
  override def reset(): Unit = {
    result = CommonConstant.AGGR_RESULT.toString
  }

  /**
    * add: 操作数据累加方法实现，session_count=0|1s_3s=2|4s_6s...|60=0
    *
    * @param v ,如：session_count
    */
  override def add(v: String): Unit = {
    val v1 = result
    val v2 = v

    if (StringUtils.isNotEmpty(v1) && StringUtils.isNotEmpty(v2)) {
      var newResult = ""
      // 从v1中，提取v2对应的值，并累加
      //0
      val oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2)

      if (oldValue != null) {
        val newValue = oldValue.toInt + 1
        //session_count=1|1s_3s=0|4s_6s...|60=0
        newResult = StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue))
      }
      result = newResult
    }
  }

  /**
    * 合并数据：别的Executor进程中的累加器的实例中封装的数据与当前exctutor进程中累加器的实例中封装的数据进行合并操作
    *
    * @param other 别的Executor进程中的累加器的实例
    */
  override def merge(other: AccumulatorV2[String, String]): Unit = other match {
    case otherAccu: SessionAggrStatAccumulator =>
      //      如：当前，session_count=4|1s_3s=3|4s_6s...|60=0
      //            别的，session_count=3|1s_3s=2|4s_6s...|60=0
      //  ~>合并后：session_count=7|1s_3s=5|4s_6s...|60=0,替换当前的result
      //切割成数组
      val thisArr: Array[String] = this.result.split("\\|")
      val otherArr: Array[String] = otherAccu.result.split("\\|")
      //字符串结构一样，使用单重for循环即可
      for (i <- 0 until thisArr.length) {
        val thisSonArr: Array[String] = thisArr(i).split("=")
        val thisKey = thisSonArr(0)
        val thisValue = thisSonArr(1).toInt

        val otherSonArr: Array[String] = otherArr(i).split("=")
        val otherKey = otherSonArr(0)
        val otherValue = otherSonArr(1).toInt

        if (thisKey.equals(otherKey)) {
          val newValue = thisValue + otherValue
          //替换
          result = StringUtils.setFieldInConcatString(result, "\\|", thisKey, newValue.toString)
        }
      }

    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  /**
    * AccumulatorV2对外访问的数据结果
    *
    * @return
    */
  override def value: String = result
}