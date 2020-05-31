package com.merkey.realtime.ad

import java._
import java.util.Date

import com.merkey.common.CommonConstant
import com.merkey.dao.ad.impl._
import com.merkey.dao.ad._
import com.merkey.entity.ad._
import com.merkey.utils.{DateUtils, ResourceManagerUtil}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Description：广告流实时统计<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年12月27日  
  *
  * @author merkey
  * @version : 1.0
  */
object AdRealtimeCalApplication {



  def main(args: Array[String]): Unit = {
    //前提：
    //从Kafka消息队列中拉取实时产生的消息
    val result = getRealTimeDataFromKafka(args)
    val sc = result._1
    val batchDuration = result._2
    val ckPathParam = result._3


    val ssc = new StreamingContext(sc, batchDuration)

    ssc.checkpoint(ckPathParam)

    // KafkaUtils.createDirectStream返回的结果是DStream，DStream中每个元素时对偶元组，key:消息的key, value:消息的内容。若是没有指定key，默认值就是：null
    val ds: DStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
      Map("metadata.broker.list" -> "NODE01:9092,NODE02:9092,NODE03:9092"), Set("ad_log"))

    //1、实时计算各batch中的每天各用户对各广告的点击次数
    //2、使用高性能方式将每天各用户对各广告的点击次数写入MySQL中（更新）
    val perADUserCliCntDS: DStream[((String, String, String), Long)] = calPerBatchUserAdCliCnt(ds)

    //3、使用filter过滤出每天对某个广告点击超过100次的黑名单用户，并写入MySQL中
    filterBlackList2DB(perADUserCliCntDS)

    //4、使用transform操作，对每个batch RDD进行处理，都动态加载MySQL中的黑名单生成RDD，然后进行join后，过滤掉batch RDD中的黑名单用户的广告点击行为
    val whiteListDS: DStream[String] = getAllWhiteListDS(ds)


    //5、使用updateStateByKey操作，实时计算每天各省各城市各广告的点击量，并实时更新到MySQL
    val perDayProvinceCityAdCntDS: DStream[(String, Long)] = calPerDayProvinceCityAdCnt(whiteListDS)


    //6、统计每天各省份top3热门广告
    calPerDayProvinceTop3(perDayProvinceCityAdCntDS)

    //7、使用window操作，对最近1小时滑动窗口内的数据，计算出各广告各分钟的点击量，并更新到MySQL中
    calRecent1HourAdCnt(whiteListDS)

    //启动
    ssc.start

    //等待结束
    ssc.awaitTermination

  }


  /**
    * 使用window操作，对最近1小时滑动窗口内的数据，计算出各广告各分钟的点击量，并更新到MySQL中
    *
    * @param whiteListDS
    */
  def calRecent1HourAdCnt(whiteListDS: DStream[String]) = {
    // 为了测试方便：窗口长度1小时~> 1分钟
    //                      时间间隔1分钟 ~>4秒钟
    //将下述DStream中的每个元素变形成：date + "_" + province + "_" + city + "_" + ad_id + "_" + user_id ~>(广告id_年月日时分,1L)
    whiteListDS.map(perEle => {
      val arr = perEle.split("_")
      var ad_id = arr(3).trim
      var date = DateUtils.formatTimeMinute(new Date(arr(0).trim.toLong))
      (ad_id + "_" + date, 1L)
    }).reduceByKeyAndWindow((v1: Long, v2: Long) => v1 + v2, Minutes(1), Seconds(4))
      .foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          rdd.foreachPartition(itr => {
            if (!itr.isEmpty) {
              val dao: IAdClickTrendDao = new IAdClickTrendDaoImpl
              val beans: util.List[AdClickTrend] = new util.LinkedList[AdClickTrend]

              itr.foreach(perEle => {
                val arr = perEle._1.split("_")
                val ad_id = arr(0).trim.toInt
                val date = arr(1).trim
                val minute = date.substring(date.length - 2)
                val click_count = perEle._2.toInt
                beans.add(new AdClickTrend(date: String, ad_id: Int, minute: String, click_count: Int))
              })

              //保存
              dao.batchDealWith(beans)

            }
          })
        }
      })
  }

  /**
    * 使用transform结合Spark SQL，统计每天各省份top3热门广告：首先以每天各省各城市各广告的点击量数据作为基础，首先统计出每天各省份各广告的点击量；
    * 然后启动一个异步子线程，使用Spark SQL动态将数据RDD转换为DataFrame后，注册为临时表；最后使用Spark SQL开窗函数，统计出各省份top3热门的广告，
    * 并更新到MySQL中
    *
    * @param perDayProvinceCityAdCntDS
    */
  def calPerDayProvinceTop3(perDayProvinceCityAdCntDS: DStream[(String, Long)]) = {
    //下述DStream中每个元素，形如： (date + "_" + province + "_" + city + "_" + ad_id, 迄今为止聚合后的结果)
    //                                       转换成： (date + "_" + province + "_" + ad_id, 迄今为止聚合后的结果)
    perDayProvinceCityAdCntDS.map(perEle => {
      val clickCount = perEle._2
      val arr = perEle._1.split("_")
      val date = arr(0).trim
      val province = arr(1).trim
      val ad_id = arr(3).trim
      (date + "_" + province + "_" + ad_id, clickCount)
    }).reduceByKey(_ + _)//注意：基于perDayProvinceCityAdCntDS进行计算，该DStream是updateStateByKey的结果。
      .foreachRDD(rdd => {
        //将rdd中的数据映射为一张虚拟表，据此求top3，并落地到db中
        if (!rdd.isEmpty()) {
          //下述RDD (date + "_" + province + "_" + ad_id, 迄今为止聚合后的结果)
          val tmpRDD: RDD[Row] = rdd.map(perEle => {
            val arr = perEle._1.split("_")
            val click_count = perEle._2.toInt
            val date = arr(0).trim
            val province = arr(1).trim
            val ad_id = arr(2).trim.toInt
            //date	province		ad_id	click_count
            Row(date, province, ad_id, click_count)
          })

          //将变形的RDD映射为一张虚拟表
          val sqlContext: SQLContext = new SQLContext(tmpRDD.sparkContext)

          //用来定制虚拟表的元数据信息，就是虚拟表的字段名以及类型信息
          val structType = StructType(Seq(StructField("date", StringType, false),
            StructField("province", StringType, false),
            StructField("ad_id", IntegerType, false),
            StructField("click_count", IntegerType, false)
          ))



          sqlContext.createDataFrame(tmpRDD, structType).createOrReplaceTempView("tmp_ad_province_top3")
          sqlContext.cacheTable("tmp_ad_province_top3")

          //针对虚拟表tmp_ad_province_top3分组求top3
          // SparkSession.getActiveSession.get
          val df: DataFrame = sqlContext.sql(
            """
              | select *,
              | row_number() over (distribute by province sort by click_count desc ) level
              | from tmp_ad_province_top3
              | having level<=3
            """.stripMargin)

          //为了测试方便，在控制台实时显示结果
          df.show(1000000)


          val resultRDD: RDD[Row] = df.rdd

          //将结果RDD中的数据落地到db中
          val dao: IProvinceTop3Dao = new AdProvinceTop3DaoImpl
          val beans: util.List[AdProvinceTop3] = new util.ArrayList[AdProvinceTop3]
          // Return an array that contains all of the elements in this RDD.
          //将当前rdd在别的partition中的数据都拉取到当前线程（不是Driver进程的main线程！！）中来
          resultRDD.collect().foreach(row => {
            val date = row.getAs[String]("date")
            val province = row.getAs[String]("province")
            val ad_id = row.getAs[Integer]("ad_id")
            val click_count = row.getAs[Integer]("click_count").toInt

            val bean: AdProvinceTop3 = new AdProvinceTop3(date: String, province: String, ad_id: Int, click_count: Int)
            beans.add(bean)
          })
          dao.batchDealWith(beans);
        }
      })
  }

  /**
    * 使用updateStateByKey操作，实时计算每天各省各城市各广告的点击量，并实时更新到MySQL
    *
    * 注意：最终定性为黑名单的用户，在没有拉黑之前，也会当成白名单进行后续的计算
    *
    * @param whiteListDS
    * @return
    */
  def calPerDayProvinceCityAdCnt(whiteListDS: DStream[String]): DStream[(String, Long)] = {
    //思路：
    //① 基于上一步所求得的白名单进行分析
    //② 将DStream中的每个元素转换成对偶元组，形如：(天_省_市_广告id，1L)
    //DStream中每个元素形如：1546850151980_浙江_丽水_4_45
    //③  使用updateStateByKey算子操作步骤2的DStream
    val perDayProvinceCityAdCntDS: DStream[(String, Long)] = whiteListDS.map(perEle => {
      val arr = perEle.split("_")
      val date = DateUtils.formatDate(new Date(arr(0).trim.toLong))
      val province = arr(1).trim
      val city = arr(2).trim
      val ad_id = arr(3).trim
      (date + "_" + province + "_" + city + "_" + ad_id, 1L)
    }).updateStateByKey((currentBatch: Seq[Long], historyBatchResult: Option[Long]) => {
      val currentBatchSum = currentBatch.sum
      val historyBatch = historyBatchResult.getOrElse(0L)
      Some(currentBatchSum + historyBatch)
    })

    //④ 保存或是更新到MySQL
    perDayProvinceCityAdCntDS.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(itr => {
          if (!itr.isEmpty) {
            val dao: IAdStatDao = new AdStatDaoImpl
            val beans: util.List[AdStat] = new util.LinkedList[AdStat]

            itr.foreach(perEle => {
              val arr = perEle._1.split("_")
              val click_count = perEle._2.toInt
              val date = arr(0).trim
              val province = arr(1).trim
              val city = arr(2).trim
              val ad_id = arr(3).trim.toInt
              val bean = new AdStat(date: String, province: String, city: String, ad_id: Int, click_count: Int)
              beans.add(bean)
            })
            dao.batchDealWith(beans)
          }
        })
      }
    })

    //返回结果
    perDayProvinceCityAdCntDS
  }


  /**
    * 找出所有的白名单用户点击的广告信息
    *
    * @param ds
    * @return
    */
  def getAllWhiteListDS(ds: DStream[(String, String)])  = {
    //根据从kafka集群中获取的DStream进行分析，从该DStream中过滤出所有白名单用户
    val whiteListDS: DStream[String] = ds.transform(rdd => {
      //①动态加载MySQL中的黑名单生成RDD
      val dao: IAdBlackListDao = new AdBlackListDaoImpl
      val beans: java.util.List[AdBlackList] = dao.findAll()

      //将java中的集合之List中的元素值取出来，置于scala中的集合之ArrayBuffer
      val tempContainer: ArrayBuffer[String] = new ArrayBuffer[String]

      for (index <- 0 until beans.size) {
        tempContainer.append(beans.get(index).getUser_id.toString)
      }

      //注意：a)不能new SparkContext,而是从rdd实例中获取其所在的SparkContext实例信息
      //         b)将java中的List集合转换为scala中的集合
      val blackListRDD: RDD[(String, String)] = rdd.sparkContext.parallelize(tempContainer).map((_, ""))


      //②然后进行join （左外连接查询）
      //a)需要将rdd中每个元素的类型转换为： （user_id,其余），如：（44，1546850151980 浙江 丽水 44 5）
      //rdd中每个元素形如：（null,1546850151980 浙江 丽水 40 5）
      val changeAfterRDD: RDD[(String, String)] = rdd.map(perEle => {
        val arr = perEle._2.split("\\s+")
        val user_id = arr(3).trim
        val date = arr(0).trim
        val province = arr(1).trim
        val city = arr(2).trim
        val ad_id = arr(4).trim
        (user_id, date + "_" + province + "_" + city + "_" + ad_id + "_" + user_id)
      })

      // RDD[(K, (V, Option[W]))]  ,where id=xx.id
      val allRDD: RDD[(String, (String, Option[String]))] = changeAfterRDD.leftOuterJoin(blackListRDD)

      //③过滤掉batch RDD中的黑名单用户的广告点击行为，求白名单
      //（44，1546850151980 浙江 丽水 44 5）要与：(44,"")进行左外连接
      //结果：存在~> (44，（1546850151980 浙江 丽水 44 5，Option(“”)）)
      //          不存在~> (44，（1546850151980_浙江_丽水_44_5，None）)  ~>在黑名单中找不到这样的元素即为白名单
      val filterAfterRDD: RDD[(String, (String, Option[String]))] = allRDD.filter(_._2._2 == None)

      //④返回白名单RDD
      val whiteListRDD: RDD[String] = filterAfterRDD.map(_._2._1)

      whiteListRDD
    })

    //whiteListDS.print(10000)
    //返回白名单
    whiteListDS
  }

  /**
    * 使用filter过滤出每天对某个广告点击超过100次的黑名单用户，并写入MySQL中
    *
    * @param perADUserCliCntDS
    */
  def filterBlackList2DB(perADUserCliCntDS: DStream[((String, String, String), Long)]): Unit = {
    //从资源文件中读取到的 黑名单判定标准：每天对某个广告点击超过100次的用户
    val performCnt = ResourceManagerUtil.getPropertiesValueByKey(CommonConstant.BLACK_LIST_CNT).toInt

    //黑名单
    val blackListDS: DStream[((String, String, String), Long)] = perADUserCliCntDS.filter(_._2 > performCnt)

    //将黑名单用户信息落地到db中
    blackListDS.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(itr => {
          if (!itr.isEmpty) {
            val dao: IAdBlackListDao = new AdBlackListDaoImpl
            val beans: java.util.List[AdBlackList] = new java.util.LinkedList[AdBlackList]

            itr.foreach(perEle => {
              val key = perEle._1
              val user_id =key._2.toInt
              beans.add(new AdBlackList(user_id))
            })

            dao.insertBatchDealWith(beans)
          }
        })
      }
    })
  }

  /**
    * 实时从kafka中读取消息，并将每条消息变形
    *
    * @param ds
    * @return
    */
  def changeDSWithMap(ds: DStream[(String, String)]): DStream[((String, String, String), Long)] = {
    //上述DStream中每个元素形如：（null,1546830907207  辽宁 鞍山 85 6）
    val dsChange2Tuple: DStream[((String, String, String), Long)] = ds.map(perEle => {
      val msg = perEle._2
      // 万能正则表达式\\s+，匹配空格：半角空格，全角空格，\t键
      val arr = msg.split("\\s+")
      //一定只精确到日
      val date = DateUtils.formatDate(new Date(arr(0).trim.toLong))
      val user_id = arr(3).trim
      val ad_id = arr(4).trim
      ((date, user_id, ad_id), 1L)
    })

    dsChange2Tuple
  }

  /**
    *
    * 计算出迄今为止每天每个用户对各个广告点击的总次数
    *
    * @param dsChange2Tuple
    * @return
    */
  def calPerADUserCliCnt(dsChange2Tuple: DStream[((String, String, String), Long)]): DStream[((String, String, String), Long)] =
    dsChange2Tuple.updateStateByKey((currentBatch: Seq[Long], historyValue: Option[Long]) => Some(currentBatch.sum + historyValue.getOrElse(0L)))

  /**
    *
    * 实时计算每天各个用户对各个广告点击的总的次数
    *
    * @param ds
    * @return
    */
  def calPerBatchUserAdCliCnt(ds: DStream[(String, String)]) = {
    //①将DStream中每个元素转换成（key,value） ~> key：天_用户id_广告id, value：1
    val dsChange2Tuple: DStream[((String, String, String), Long)] = changeDSWithMap(ds)

    //②使用updateStateBykey算子将迄今为止，各用户对各广告的点击次数聚合起来(根据key聚合，将对应的值累加起来)
    val perADUserCliCntDS: DStream[((String, String, String), Long)] = calPerADUserCliCnt(dsChange2Tuple)

    //    perADUserCliCntDS.print(10000)

    //③将聚合后的结果落地到db中
    savePerADUserCliCntToDB(perADUserCliCntDS)

    //④返回
    perADUserCliCntDS

  }

  /**
    * 将聚合后的结果落地到db中
    *
    * 使用高性能方式将每天各用户对各广告的点击次数写入MySQL中
    *
    * @param perADUserCliCntDS
    */
  def savePerADUserCliCntToDB(perADUserCliCntDS: DStream[((String, String, String), Long)]) = {
    //DStream中每个元素形如：((2019-01-07,2,4),8)
    perADUserCliCntDS.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(itr => {
          if (!itr.isEmpty) {
            val dao: IAdUserClickCountDao = new AdUserClickCountDaoImpl
            val beans: java.util.List[AdUserClickCount] = new java.util.LinkedList[AdUserClickCount]

            //经过下述循环，已经将迭代器中所有的元素置于到容器中
            itr.foreach(perEle => {
              val key = perEle._1
              val click_count = perEle._2.toInt

              val date = key._1
              val user_id = key._2.toInt
              val ad_id = key._3.toInt

              val bean: AdUserClickCount = new AdUserClickCount(date: String, user_id: Int, ad_id: Int, click_count: Int);
              beans.add(bean)
            })

            //批量操作
            dao.batchDealWith(beans)
          }
        })
      }
    })
  }


  /**
    * 从kafka集群中实时拉取数据
    *
    * @return
    */
  def getRealTimeDataFromKafka(args: Array[String]) = {
    //拦截非法的操作
    if (args == null || args.length != 2) {
      println("警告！参数没有设置！请传入参数！如：批次的间隔时间 checkPoint的Path")
      System.exit(-1)
    }


    //动态获得传入的参数
    //①批次的间隔时间
    val timeIntervalParam = args(0).toInt

    //②checkPoint的Path
    val ckPathParam = args(1).trim


    val conf: SparkConf = new SparkConf()
    //判断当前job运行的模式，若是local，手动设置；若是test，production模式，通过spark-submit --master xx
    if ("local".equals(ResourceManagerUtil.runMode.name().toLowerCase())) {
      conf.setMaster("local[*]")
    }
    conf.setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)
    //设置日志的级别
    sc.setLogLevel("WARN")

    val batchDuration: Duration = Seconds(timeIntervalParam)


    //ds.print(200000)
    //返回结果
    (sc, batchDuration, ckPathParam)
  }
}
