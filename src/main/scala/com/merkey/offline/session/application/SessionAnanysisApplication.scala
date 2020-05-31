package com.merkey.offline.session.application

import java._
import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.merkey.common.CommonConstant
import com.merkey.dao.common.impl.ITaskDaoImpl
import com.merkey.dao.session.impl._
import com.merkey.dao.session._
import com.merkey.entity.common.{SearchParam, Task}
import com.merkey.entity.session._
import com.merkey.offline.session.accumulator.SessionAggrStatAccumulator
import com.merkey.offline.session.entity.SelfCategoryBean
import com.merkey.utils.{ResourceManagerUtil, StringUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Description：Session访问分析<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年12月25日  
  *
  * @author merkey
  * @version : 1.0
  */
object SessionAnanysisApplication {



  def main(args: Array[String]): Unit = {

    //步骤
    //前提：
    val spark: SparkSession = commonOperate(args)

    //1,按条件（公司决策人员）筛选session
    val rdd: RDD[Row] = filterSessionByCondition(args, spark)

    //2,统计出符合条件的session中，访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、10m~30m、30m以上各个范围内的session占比；
    // 访问步长在1~3、4~6、7~9、10~30、30~60、60以上各个范围内的session占比
    //calTimeLenAndStepLenRate(spark, args)

    //3, 在符合条件的session中，按照时间比例随机抽取1000个session
    //randomExtract1000Session(spark, args)

    //4,在符合条件的session中，获取点击、下单和支付数量排名前10的品类
    val categoryIdContainer: ArrayBuffer[Int] = calClickAndOrderAndPayTop10(spark, args, rdd)

    //5,对于排名前10的品类，分别获取其点击次数排名前10的session
    calTop10CategoryTop10Session(spark, args, categoryIdContainer)

    //资源释放
    spark.stop

  }


  /**
    * 对于排名前10的品类，分别获取其点击次数排名前10的session
    *
    * @param spark
    * @param args
    * @param categoryIdContainer
    */
  def calTop10CategoryTop10Session(spark: SparkSession, args: Array[String], categoryIdContainer: ArrayBuffer[Int]) = {
    //步骤：
    //①从上一步中获取排名前10的品类的id
    //②通过循环来析排名前十的每一个品类
    val dao: ITop10CategorySessionDao = new Top10CategorySessionDaoImpl
    val beans: java.util.List[Top10CategorySession] = new java.util.LinkedList[Top10CategorySession]


    val daoDetail: ISessionDetailDao = new SessionDetailDaoImpl
    val sessionDetialBeans: java.util.List[SessionDetail] = new java.util.LinkedList[SessionDetail]


    for (categoryId <- categoryIdContainer) {
      //i)每循环一次，求出该品类点击次数排名前10的session
      spark.sql("select session_id, count(*)  click_count  from filter_after_visit_action where click_category_id=" + categoryId + " group by session_id")
        .createOrReplaceTempView("temp_top10_session")
      val arr: Array[Row] = spark.sql("select * from temp_top10_session order by click_count desc limit 10").rdd.collect()

      //ii)将当前品类点击数排名前十的session信息保存到top10_category_session表中
      saveNowCategoryTop10Session(beans, arr, args, categoryId, dao)

      //iii)将top10品类的session的明细数据取出来，保存到db中去（表：session_detail）
      saveTop10DetailToDB(spark, args, daoDetail, sessionDetialBeans, categoryId)
    }
  }

  /**
    * 将top10品类的session的明细数据取出来，保存到db中去（表：session_detail）
    *
    * @param spark
    * @param args
    * @param daoDetail
    * @param sessionDetialBeans
    */
  def saveTop10DetailToDB(spark: SparkSession, args: Array[String], daoDetail: ISessionDetailDao, sessionDetialBeans: util.List[SessionDetail], categoryId: Int) = {
    val sessionDetialArr: Array[Row] = spark.sql(" select *  from filter_after_visit_action where click_category_id=" + categoryId).rdd.collect()

    //清空容器
    sessionDetialBeans.clear()
    for (row <- sessionDetialArr) {
      //注意：若是整型的字段，在spark sql中默认都是Long类型
      val task_id = args(0).toInt
      val user_id = row.getAs[Long]("user_id").toInt
      val session_id = row.getAs[String]("session_id")
      val page_id = row.getAs[Long]("page_id").toInt
      val action_time = row.getAs[String]("action_time")
      val search_keyword = row.getAs[String]("search_keyword")
      val click_category_id = row.getAs[Long]("click_category_id").toInt
      val click_product_id = row.getAs[Long]("click_product_id").toInt
      val order_category_ids = row.getAs[String]("order_category_ids")
      val order_product_ids = row.getAs[String]("order_product_ids")
      val pay_category_ids = row.getAs[String]("pay_category_ids")
      val pay_product_ids = row.getAs[String]("pay_product_ids")
      val bean = new SessionDetail(task_id: Int, user_id: Int, session_id: String, page_id: Int, action_time: String, search_keyword: String, click_category_id: Int, click_product_id: Int, order_category_ids: String, order_product_ids: String, pay_category_ids: String, pay_product_ids: String)
      //判断当前的明细数据在表中是否存在，若不存在，添加到容器中进行批量新增处理
      if (!daoDetail.queryAll().contains(bean)) {
        sessionDetialBeans.add(bean)
      }
    }
    daoDetail.insertBatch(sessionDetialBeans)
  }

  /**
    * 将当前品类点击数排名前十的session信息保存到top10_category_session表中
    *
    * @param beans
    * @param arr
    * @param args
    * @param categoryId
    * @param dao
    */
  def saveNowCategoryTop10Session(beans: java.util.List[Top10CategorySession], arr: Array[Row], args: Array[String], categoryId: Int, dao: ITop10CategorySessionDao) = {
    //清空容器
    beans.clear()
    for (row <- arr) {
      val task_id = args(0).toInt
      val category_id = categoryId
      val session_id = row.getAs[String]("session_id")
      val click_count = row.getAs[Long]("click_count").toInt
      beans.add(new Top10CategorySession(task_id: Int, category_id: Int, session_id: String, click_count: Int))
    }
    dao.insertBatch(beans)
  }

  /**
    * 在符合条件的session中，获取点击、下单和支付数量排名前10的品类
    *
    * @param spark
    * @param args
    * @param rdd
    * @return
    */
  def calClickAndOrderAndPayTop10(spark: SparkSession, args: Array[String], rdd: RDD[Row]): ArrayBuffer[Int] = {
    //前提：设计一个实体类，继承Trait之Ordered (类似于Java中的Comparable)
    //步骤：
    //设计一个容器，用于存储点击、下单和支付数量排名前10的品类的id信息
    val categoryIdContainer: ArrayBuffer[Int] = new ArrayBuffer

    //①求出排名前十的商品的点击次数
    val top10Arr: Array[Row] = spark.sql("select click_category_id category_id,count(*) total_click_category_cnt from filter_after_visit_action  where click_category_id is not null  group by click_category_id ").rdd.collect()

    //②分析步骤①的结果，据此求出当前品类的下单总数，支付总数
    //准备一个容器
    val container: ArrayBuffer[SelfCategoryBean] = new ArrayBuffer

    for (row <- top10Arr) {
      //当前品类的id
      val category_id = row.getAs[Long]("category_id")

      //当前品类的总的点击次数
      val total_click_category_cnt = row.getAs[Long]("total_click_category_cnt")

      //当前品类的总的下单次数
      val total_order_category_cnt = rdd.filter(row => category_id.toString.equals(row.getAs[String]("order_category_ids"))).count()

      //当前品类的总的支付次数
      val total_pay_category_cnt = rdd.filter(row => category_id.toString.equals(row.getAs[String]("pay_category_ids"))).count()

      //封装成一个Bean
      val bean = new SelfCategoryBean(category_id.toInt: Int, total_click_category_cnt.toInt: Int, total_order_category_cnt.toInt: Int, total_pay_category_cnt.toInt: Int)

      //添加到容器中
      container.append(bean)
    }

    //③将容器转换成rdd,将rdd中每个元素转换成对偶元组（原因：可以使用sortByKey算子求topN）
    //   for(bean<-container){
    //     println(bean)
    //   }
    val arr: Array[(SelfCategoryBean, String)] = spark.sparkContext.parallelize(container).map((_, "")).sortByKey(true, 1).take(10)

    arr.foreach(perEle => println(perEle._1))

    //④将结果落地到db中
    saveTop10ToDB(arr, args, categoryIdContainer)

    //⑤返回结果
    categoryIdContainer
  }

  /**
    * 将top10品类的信息（点击总数，下单总数，支付总数）结果落地到db中
    *
    * @param arr
    * @param args
    * @param categoryIdContainer
    */
  def saveTop10ToDB(arr: Array[(SelfCategoryBean, String)], args: Array[String], categoryIdContainer: ArrayBuffer[Int]) = {

    //a)将arr中的top10的数据存入到java中的容器List中
    val beans: java.util.List[Top10Category] = new java.util.LinkedList[Top10Category]
    val task_id = args(0).toInt
    arr.foreach(perEle => {
      val tempBean: SelfCategoryBean = perEle._1
      val category_id: Int = tempBean.getCategory_id
      val click_count: Int = tempBean.getTotal_click_category_cnt
      val order_count: Int = tempBean.getTotal_order_category_cnt
      val pay_count: Int = tempBean.getTotal_pay_category_cnt
      val bean = new Top10Category(task_id: Int, category_id: Int, click_count: Int, order_count: Int, pay_count: Int)
      beans.add(bean)

      //将当前品类的id保存起来
      categoryIdContainer.append(category_id)
    })

    //b)保存
    val dao: ITop10CategoryDao = new Top10CategoryDaoImpl
    dao.insertBatch(beans)
  }

  /**
    * 找出待抽取的数据，然后根据比率值抽样，且将抽样后的结果保存到db中，且保存到明细表中
    *
    * @param spark
    * @param totalSessionCnt
    * @param args
    * @param perPeriodArr
    */
  def searchWillDataAndSample(spark: SparkSession, totalSessionCnt: Long, args: Array[String], perPeriodArr: Array[Row]): Unit = {
    //通过循环分析步骤①中的rdd，每循环一次，分析一个时间段内的session,并从中根据指定的比率值进行抽样
    perPeriodArr.foreach(row => { //perPeriodArr类型是Array[Row]，foreach在Driver进程中运行的
      //a)获得时间段
      val timePeriod = row.getAs[String]("timePeriod") //2019-12-23 12   50%  1000万（该时段内的日志条数），那么，该时段内抽样后的数据条数是500

      //获得比率值
      val rateValue: java.math.BigDecimal = row.getAs[java.math.BigDecimal]("rateValue")
      val tmpRateValue = rateValue.doubleValue()

      //从指定的时段内获取待抽取的数据
      val willSampleRDD: RDD[Row] = spark.sql("select session_id,action_time,search_keyword,click_category_id,user_id,page_id,click_product_id,order_category_ids,order_product_ids,pay_category_ids,pay_product_ids from filter_after_visit_action where instr(action_time,'" + timePeriod + "')>0").rdd

      //b)样本数
      var sampleCnt: Int = Integer.parseInt(ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.SAMPLE_CNT))
      sampleCnt = if (totalSessionCnt.toInt > sampleCnt) {
        // println("总的session数>1000")
        sampleCnt
      } else {
        totalSessionCnt.toInt
      }


      //c)正式抽样
      val arr: Array[Row] = willSampleRDD.takeSample(true, (sampleCnt * tmpRateValue).toInt)

      //d)将抽样的结果根据session_id分组后保存到db中
      //i)将抽样后的结果转换成rdd
      val rddTmp: RDD[Row] = spark.sparkContext.parallelize(arr)
      //ii)将rdd映射为一张虚拟表
      val structType = StructType(Seq(StructField("session_id", StringType, false),
        StructField("action_time", StringType, false),
        StructField("search_keyword", StringType, true),
        StructField("click_category_id", LongType, true)))
      spark.createDataFrame(rddTmp, structType).createOrReplaceTempView("tmp_random_sample")
      spark.sqlContext.cacheTable("tmp_random_sample")

      //iii）然后进行聚合
      val aggregateRDD: RDD[Row] = spark.sql(
        """
          |select session_id,
          |   min(action_time) start_time,
          |   max(action_time)  end_time,
          |   concat_ws(',',collect_set( search_keyword)) search_keywords,
          |   concat_ws(',',collect_set(if(click_category_id='null' or click_category_id is null,'',click_category_id))) click_category_ids
          |from tmp_random_sample group by session_id
        """.stripMargin
      ).rdd

      //iiii)将聚合后的结果落地到db中
      saveSampleAndAggregateToDB(args, aggregateRDD)

      //e)随机抽取出来的session的明细数据保存到明细表中
      saveDetailDataToDB(args, arr)

      //释放cache中的表
      spark.sqlContext.uncacheTable("tmp_random_sample")
    })
  }

  /**
    * 将聚合后的结果落地到db中
    *
    * @param args
    * @param aggregateRDD
    */
  def saveSampleAndAggregateToDB(args: Array[String], aggregateRDD: RDD[Row]) = {
    aggregateRDD.foreachPartition(itr => {
      if (!itr.isEmpty) {

        val dao: ISessionRandomExtract = new SessionRandomExtractImpl
        val beans: java.util.List[SessionRandomExtract] = new java.util.LinkedList[SessionRandomExtract]

        itr.foreach(row => {
          //获取当前行的数据
          val task_id = args(0).toInt
          val session_id = row.getAs[String]("session_id")
          val start_time = row.getAs[String]("start_time")
          val end_time = row.getAs[String]("end_time")
          val click_category_ids = row.getAs[String]("click_category_ids")
          val search_keywords = row.getAs[String]("search_keywords")
          //封装成一个bean
          val bean: SessionRandomExtract = new SessionRandomExtract(task_id: Int, session_id: String, start_time: String, end_time: String, search_keywords: String, click_category_ids: String)

          //添加到容器中
          beans.add(bean)
        })

        //批量新增
        dao.insertBatch(beans)
      }
    })
  }


  /**
    *
    * 随机抽取出来的session的明细数据保存到明细表中
    *
    * @param args
    * @param arr
    */
  def saveDetailDataToDB(args: Array[String], arr: Array[Row]) = {
    val dao: ISessionDetailDao = new SessionDetailDaoImpl
    val beans: java.util.List[SessionDetail] = new java.util.LinkedList[SessionDetail]

    for (row <- arr) {
      val task_id = args(0).toInt
      //注意：若是整型的字段，在spark sql中默认都是Long类型
      val user_id = row.getAs[Long]("user_id").toInt
      val session_id = row.getAs[String]("session_id")
      val page_id = row.getAs[Long]("page_id").toInt
      val action_time = row.getAs[String]("action_time")
      val search_keyword = row.getAs[String]("search_keyword")
      val click_category_id = row.getAs[Long]("click_category_id").toInt
      val click_product_id = row.getAs[Long]("click_product_id").toInt
      val order_category_ids = row.getAs[String]("order_category_ids")
      val order_product_ids = row.getAs[String]("order_product_ids")
      val pay_category_ids = row.getAs[String]("pay_category_ids")
      val pay_product_ids = row.getAs[String]("pay_product_ids")

      val bean = new SessionDetail(task_id: Int, user_id: Int, session_id: String, page_id: Int, action_time: String, search_keyword: String, click_category_id: Int, click_product_id: Int, order_category_ids: String, order_product_ids: String, pay_category_ids: String, pay_product_ids: String)
      beans.add(bean)
    }
    //批量保存
    dao.insertBatch(beans)
  }

  /**
    * 在符合条件的session中，按照时间比例随机抽取1000个session
    *
    * @param spark
    * @param args
    */
  def randomExtract1000Session(spark: SparkSession, args: Array[String]): Unit = {
    //步骤：
    //①求出各个时段内session数的比率值,下述totalSessionCnt中存储的是不排重的session的总数
    val totalSessionCnt: Long = spark.sql("select count(*) totalSessionCnt from filter_after_visit_action").first().getAs[Long]("totalSessionCnt")

    val perPeriodArr: Array[Row] = spark.sql("select  substring( action_time,1,13) timePeriod ,count(*)/" + totalSessionCnt.toDouble + " rateValue  from filter_after_visit_action group by substring( action_time,1,13)").rdd.collect()


    //②找出待抽取的数据，然后根据比率值抽样
    searchWillDataAndSample(spark, totalSessionCnt, args, perPeriodArr)
  }

  /**
    * 根据给定的开始时间和结束时间，计算时长
    *
    * 注意点：若是将下述方法抽取到工具类中，设置为static的，被所有的task线程共享，多线程并发访问存在着冲突
    *
    * @param startTimeStr ，如：2018-12-29 20:44:20
    * @param endTimeStr
    * @return
    */
  def calTimeLen(startTimeStr: String, endTimeStr: String): Long = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val startTime = sdf.parse(startTimeStr)
    val endTime = sdf.parse(endTimeStr)
    endTime.getTime - startTime.getTime
  }

  /**
    * 分析session的步长
    *
    * @param row
    * @param selfAccu
    */
  def analysisNowSessionStepLen(row: Row, selfAccu: SessionAggrStatAccumulator) = {
    val stepLen = row.getAs[Long]("stepLen")
    //1~3、4~6、7~9、10~30、30~60、60
    if (stepLen >= 1 && stepLen <= 3) {
      selfAccu.add(CommonConstant.STEP_PERIOD_1_3)
    } else if (stepLen >= 4 && stepLen <= 6) {
      selfAccu.add(CommonConstant.STEP_PERIOD_4_6)
    } else if (stepLen >= 7 && stepLen <= 9) {
      selfAccu.add(CommonConstant.STEP_PERIOD_7_9)
    } else if (stepLen >= 10 && stepLen <= 30) {
      selfAccu.add(CommonConstant.STEP_PERIOD_10_30)
    } else if (stepLen > 30 && stepLen <= 60) {
      selfAccu.add(CommonConstant.STEP_PERIOD_30_60)
    } else if (stepLen > 60) {
      selfAccu.add(CommonConstant.STEP_PERIOD_60)
    }
  }

  /**
    * 分析session的时长
    * 计算当前session的时长在哪个范围之内，据此使用累加器进行累加操作
    *
    * @param row
    * @param selfAccu
    */
  def analysisNowSessionTimeLen(row: Row, selfAccu: SessionAggrStatAccumulator) = {
    var originalValue = row.getAs[Long]("timeLen")

    //业务（若是当前的session只有一条日志信息，也就是用户只访问了网站首页就退出了），约定在首页停留了2s
    originalValue = if (originalValue == 0) 2000 else originalValue

    val timeLenSecond = originalValue / 1000
    val timeLenMinute = timeLenSecond / 60
    // 1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、 10m~30m、30m
    if (timeLenSecond >= 1 && timeLenSecond <= 3) {
      selfAccu.add(CommonConstant.TIME_PERIOD_1s_3s)
    } else if (timeLenSecond >= 4 && timeLenSecond <= 6) {
      selfAccu.add(CommonConstant.TIME_PERIOD_4s_6s)
    } else if (timeLenSecond >= 7 && timeLenSecond <= 9) {
      selfAccu.add(CommonConstant.TIME_PERIOD_7s_9s)
    } else if (timeLenSecond >= 10 && timeLenSecond <= 30) {
      selfAccu.add(CommonConstant.TIME_PERIOD_10s_30s)
    } else if (timeLenSecond > 30 && timeLenSecond <= 60) {
      selfAccu.add(CommonConstant.TIME_PERIOD_30s_60s)
    } else if (timeLenMinute > 1 && timeLenMinute <= 3) {
      selfAccu.add(CommonConstant.TIME_PERIOD_1m_3m)
    } else if (timeLenMinute > 3 && timeLenMinute <= 10) {
      selfAccu.add(CommonConstant.TIME_PERIOD_3m_10m)
    } else if (timeLenMinute > 10 && timeLenMinute <= 30) {
      selfAccu.add(CommonConstant.TIME_PERIOD_10m_30m)
    } else if (timeLenMinute > 30) {
      selfAccu.add(CommonConstant.TIME_PERIOD_30m)
    }
  }

  /**
    * 计算各个统计维度内session的占比，并保存到db中
    *
    * @param selfAccu
    * @param args
    */
  def calRateAndSaveToDB(selfAccu: SessionAggrStatAccumulator, args: Array[String]): Unit = {

    //session_count=189|1s_3s=45|4s_6s...|60=28

    val finalResult: String = selfAccu.value
    val task_id = args(0).toInt
    val session_count = StringUtils.getFieldFromConcatString(finalResult, "\\|", CommonConstant.SESSION_COUNT).toDouble
    val period_1s_3s = Integer.parseInt(StringUtils.getFieldFromConcatString(finalResult, "\\|", CommonConstant.TIME_PERIOD_1s_3s)) / session_count
    val period_4s_6s = Integer.parseInt(StringUtils.getFieldFromConcatString(finalResult, "\\|", CommonConstant.TIME_PERIOD_4s_6s)) / session_count
    val period_7s_9s = Integer.parseInt(StringUtils.getFieldFromConcatString(finalResult, "\\|", CommonConstant.TIME_PERIOD_7s_9s)) / session_count
    val period_10s_30s = Integer.parseInt(StringUtils.getFieldFromConcatString(finalResult, "\\|", CommonConstant.TIME_PERIOD_10s_30s)) / session_count
    val period_30s_60s = Integer.parseInt(StringUtils.getFieldFromConcatString(finalResult, "\\|", CommonConstant.TIME_PERIOD_30s_60s)) / session_count
    val period_1m_3m = Integer.parseInt(StringUtils.getFieldFromConcatString(finalResult, "\\|", CommonConstant.TIME_PERIOD_1m_3m)) / session_count
    val period_3m_10m = Integer.parseInt(StringUtils.getFieldFromConcatString(finalResult, "\\|", CommonConstant.TIME_PERIOD_3m_10m)) / session_count
    val period_10m_30m = Integer.parseInt(StringUtils.getFieldFromConcatString(finalResult, "\\|", CommonConstant.TIME_PERIOD_10m_30m)) / session_count
    val period_30m = Integer.parseInt(StringUtils.getFieldFromConcatString(finalResult, "\\|", CommonConstant.TIME_PERIOD_30m)) / session_count
    val step_1_3 = Integer.parseInt(StringUtils.getFieldFromConcatString(finalResult, "\\|", CommonConstant.STEP_PERIOD_1_3)) / session_count
    val step_4_6 = Integer.parseInt(StringUtils.getFieldFromConcatString(finalResult, "\\|", CommonConstant.STEP_PERIOD_4_6)) / session_count
    val step_7_9 = Integer.parseInt(StringUtils.getFieldFromConcatString(finalResult, "\\|", CommonConstant.STEP_PERIOD_7_9)) / session_count
    val step_10_30 = Integer.parseInt(StringUtils.getFieldFromConcatString(finalResult, "\\|", CommonConstant.STEP_PERIOD_10_30)) / session_count
    val step_30_60 = Integer.parseInt(StringUtils.getFieldFromConcatString(finalResult, "\\|", CommonConstant.STEP_PERIOD_30_60)) / session_count
    val step_60 = Integer.parseInt(StringUtils.getFieldFromConcatString(finalResult, "\\|", CommonConstant.STEP_PERIOD_60)) / session_count

    val bean: SessionAggrStat = new SessionAggrStat(task_id: Int, session_count.toInt: Int, period_1s_3s: Double, period_4s_6s: Double, period_7s_9s: Double, period_10s_30s: Double, period_30s_60s: Double, period_1m_3m: Double, period_3m_10m: Double, period_10m_30m: Double, period_30m: Double, step_1_3: Double, step_4_6: Double, step_7_9: Double, step_10_30: Double, step_30_60: Double, step_60: Double)

    val dao: ISessionAggrStatDao = new SessionAggrStatDaoImpl
    dao.insert(bean)

  }

  /**
    * 统计出符合条件的session中,统计时长和步长占比
    *
    * 如： 1s~3s时长占比 = 1s~3s内的session总数/总的session数 = 0.23
    * 1~3步长占比 = 1~3步内的session总数/总的session数 = 0.34
    *
    * @param spark
    * @param args
    */
  def calTimeLenAndStepLenRate(spark: SparkSession, args: Array[String]): Unit = {
    //前提：准备一个自定义累加器的实例
    val selfAccu: SessionAggrStatAccumulator = new SessionAggrStatAccumulator

    //注册累加器
    spark.sparkContext.register(selfAccu)
    //println("Driver进程中的main, 线程名是："+Thread.currentThread().getName)

    //①根据session_id进行分组，可以求出每个session的时长和步长
    //注册自定义
    spark.udf.register("getTimeLen", (startTimeStr: String, endTimeStr: String) => calTimeLen(startTimeStr, endTimeStr))


    //    spark.sql(" select  session_id, count(*) stepLen,  getTimeLen(min(action_time),max(action_time)) timeLen from filter_after_visit_action group by session_id")
    //      .show(100)

    val stepAndTimeRDD: RDD[Row] = spark.sql(" select  session_id, count(*) stepLen,  getTimeLen(min(action_time),max(action_time)) timeLen from filter_after_visit_action group by session_id").rdd

    //②循环分析rdd，依次取出每个元素（session_id, stepLen,timeLen）,分别与不同的指标进行比对 ，若在范围内，那么，对应的累加器加1
    stepAndTimeRDD.foreach(row => {
      //println("~~> 线程名是："+Thread.currentThread().getName)
      //session_cont累加1
      selfAccu.add(CommonConstant.SESSION_COUNT)

      //分析session的步长
      analysisNowSessionStepLen(row, selfAccu)

      //分析session的时长
      analysisNowSessionTimeLen(row, selfAccu)
    })

    //③计算各个统计维度内session数的占比，且将其保存到db中
    calRateAndSaveToDB(selfAccu, args)
  }

  /**
    * 根据条件（task表中）筛选出数据
    *
    * @param args
    * @param spark
    * @return
    */
  def filterSessionByCondition(args: Array[String], spark: SparkSession) = {
    val task_id = args(0).toInt

    val task: Task = new ITaskDaoImpl().findTaskById(task_id)
    //System.out.println(task)

    val params = task.getTask_param;
    val searchParam: SearchParam = JSON.parseObject(params, classOf[SearchParam])
    //println(searchParam)

    //动态拼接sql语句
    val sqlBuffer = new StringBuffer
    sqlBuffer.append("select  u.* from web_log_analysis_hive.user_visit_action u, web_log_analysis_hive.user_info i where u.user_id=i.user_id  ")
    val ages = searchParam.getAges
    val genders = searchParam.getGenders
    val professionals = searchParam.getProfessionals
    val cities = searchParam.getCities
    val start_time = searchParam.getStart_time
    val end_time = searchParam.getEnd_time

    //分析ages
    if (ages != null && ages.size() > 0) {
      val minAge = ages.get(0)
      val maxAge = ages.get(1)
      sqlBuffer.append("and (i.age between ").append(minAge).append(" and ").append(maxAge).append(")")
    }

    //分析性别
    if (genders != null && genders.size() > 0) {
      //i.sex in ('男','女')
      // ['男','女']
      // SerializerFeature.UseSingleQuotes ~> 生成的json格式的数据中的字符串使用单引号括起来
      val gendersStr = JSON.toJSONString(genders, SerializerFeature.UseSingleQuotes).replace("[", "(").replace("]", ")")
      sqlBuffer.append(" and (i.sex in ").append(gendersStr).append(")")
    }

    //分析职业
    if (professionals != null && professionals.size() > 0) {
      val professionalsStr = JSON.toJSONString(professionals, SerializerFeature.UseSingleQuotes).replace("[", "(").replace("]", ")")
      sqlBuffer.append(" and (i.professional in ").append(professionalsStr).append(")")
    }

    //分析城市
    if (cities != null && cities.size() > 0) {
      val citiesStr = JSON.toJSONString(cities, SerializerFeature.UseSingleQuotes).replace("[", "(").replace("]", ")")
      sqlBuffer.append(" and (i.city in ").append(citiesStr).append(")")
    }


    //分析开始时间
    if (start_time != null && !start_time.isEmpty) {
      sqlBuffer.append(" and  (u.action_time >='").append(start_time).append("')")
    }
    //分析结束时间
    if (end_time != null && !end_time.isEmpty) {
      sqlBuffer.append(" and  (u.action_time <='").append(end_time).append("')")
    }

    //显示sql语句
    println(sqlBuffer)
    //select
    //    u.* from web_log_analysis_hive.user_visit_action u, web_log_analysis_hive.user_info i
    // where u.user_id=i.user_id  and (i.age between 0 and 100) and (i.sex in ('男','女'))
    //           and (i.professional in ('教师','工人','记者','演员','厨师','医生','护士','司机','军人','律师'))
    //           and (i.city in ('南京','无锡','徐州','常州','苏州','南通','连云港','淮安','盐城','扬州'))

    //spark.sql(sqlBuffer.toString).show(200000)
    //③将筛选出的结果注册为一张虚拟表，供后续的步骤使用 （使用缓存提高查询的效率）
    spark.sql(sqlBuffer.toString).createOrReplaceTempView("filter_after_visit_action")
    spark.sqlContext.cacheTable("filter_after_visit_action")

    //测试
    //  spark.sql("select * from filter_after_visit_action").show(500)

    //返回RDD
    spark.sql(sqlBuffer.toString).rdd
  }


  /**
    * 获得SparkSession的实例
    *
    * @param args
    * @return
    */
  def commonOperate(args: Array[String]): SparkSession = {
    //Spark高版本中，核心类：SparkSession,封装了SpardConf, SparkContext,SQLContext
    val builder = SparkSession.builder().appName(this.getClass.getSimpleName)

    //判断当前job运行的模式，若是local，手动设置；若是test，production模式，通过spark-submit --master xx
    if ("local".equals(ResourceManagerUtil.runMode.name().toLowerCase())) {
      builder.master("local[*]")
    }
    val spark: SparkSession = builder.enableHiveSupport().getOrCreate()


    //调用下述的方法，三张虚拟表已经驻留在内存中了，后续的程序直接使用即可
    //MockData.mock(spark.sparkContext, spark.sqlContext)
    //读取hive表中的数据，将其装载到内存中
    //loadDataFromHive(spark)

    //测试：访问user_visit_action虚拟表中的数据
    //spark.sql("select * from web_log_analysis_hive.user_visit_action").show(10)

    //拦截非法的操作
    if (args == null || args.length != 1) {
      print("请传入TaskId!")
      System.exit(-1)
    }

    //设置日志的级别
    spark.sparkContext.setLogLevel("WARN")


    //实例序列化的技术选用kyro
    spark.sparkContext.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[SelfCategoryBean]))


    //返回
    spark
  }


  /**
    * 读取hive表中的数据，将其装载到内存中(注意：若是hive表已经创建完毕，且数据已经加载完毕；不需要有下述的方法)
    *
    * @param spark
    */
  def loadDataFromHive(spark: SparkSession) = {
    spark.sql("DROP database if exists web_log_analysis_hive_2 cascade")
    spark.sql("create database if not exists web_log_analysis_hive_2")
    spark.sql("USE web_log_analysis_hive_2")
    spark.sql(
      """
        			|CREATE external table IF NOT EXISTS user_visit_action(
        			|	`date` string,
        			|	user_id	bigint,
        			|	session_id	string,
        			|	page_id	bigint,
        			|	action_time string,
        			|	search_keyword string,
        			|	click_category_id bigint,
        			|	click_product_id bigint,
        			|	order_category_ids string,
        			|	order_product_ids string,
        			|	pay_category_ids string,
        			|	pay_product_ids string,
        			|	city_id bigint
        			|) row format delimited
        			|fields terminated by "|"
        			|location  "hdfs://ns1/web-log-analysis/user_visit_action"
      		  """.stripMargin
    )

    spark.sql(
      """
        			|CREATE external table IF NOT EXISTS user_info(
        			|    user_id bigint,
        			|    username string,
        			|    name string,
        			|    age int,
        			|    professional string,
        			|    sex string,
        			|    city string
        			|) row format delimited
        			|fields terminated by "|"
        			|location  "hdfs://ns1/web-log-analysis/user_info"
      		  """.stripMargin
    )

    spark.sql(
      """
        			|CREATE external table IF NOT EXISTS product_info(
        			|    product_id  bigint,
        			|    product_name string,
        			|    extend_info string
        			|) row format delimited
        			|fields terminated by "|"
        			|location  "hdfs://ns1/web-log-analysis/product_info"
      		  """.stripMargin
    )
  }
}
