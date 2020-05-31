package com.merkey.offline.page

import java._

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.merkey.dao.common.impl.ITaskDaoImpl
import com.merkey.dao.page.IPageConvertRateDao
import com.merkey.dao.page.impl.PageConvertRateDaoImpl
import com.merkey.entity.common.{SearchParam, Task}
import com.merkey.entity.page.PageConvertRate
import com.merkey.offline.session.entity.SelfCategoryBean
import com.merkey.utils.{NumberUtils, ResourceManagerUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Description：页面单跳转化率统计模块 （与需求高度契合的版本）<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年12月26日  
  *
  * @author merkey
  * @version : 1.0
  */
object PageSingleJumpRateApplication {



  def main(args: Array[String]): Unit = {
    //步骤
    //前提：
    val spark: SparkSession = commonOperate(args)

    //1,按条件筛选session
    val result: (RDD[Row], util.List[Integer]) = filterSessionByCondition(args, spark)

    //2，分析rdd，求出每个页面的单条转化率
    calPageConvertRate(result, args, spark)

    //资源释放
    spark.stop
  }

  /**
    * 将页面流分解成页面切片，也即是：1,2,3 → 1_2=0|2_3=0
    * @param spark
    * @param page_flow
    * @return
    */
  def splitPageFlowAndSaveToBroacast(spark: SparkSession, page_flow: util.List[Integer]): (ArrayBuffer[String], Broadcast[ArrayBuffer[String]]) ={
    //准备一个容器，存储待分析的页面流首页的id，以及后续的所有页面切片 ~>是算子中rdd实例.reduceBykey(_+_)
    //容器中存储：1,1_2,2_3,...
    val pageSplitArr: ArrayBuffer[String] = new ArrayBuffer[String]

    //将首页的id添加到容器中
    pageSplitArr.append(page_flow.get(0).toString)

    //1, 2, 3, 4, 5, 7, 9
    //1,1_2,2_3
    for (i <- 1 until page_flow.size) {
      val prePageId = page_flow.get(i - 1)
      val currentPageId = page_flow.get(i)
      val pageSplit = prePageId + "_" + currentPageId
      pageSplitArr.append(pageSplit)
    }

    //将将页面流切割成页面切片，置于广播变量中，供后续分析各个session时使用
    val bcPageSplitArr: Broadcast[ArrayBuffer[String]] = spark.sparkContext.broadcast(pageSplitArr)

    //println("首页，以及后续的页面切片如下：")
    //pageSplitArr.foreach(println)

    (pageSplitArr, bcPageSplitArr)
  }

  /**
    * 计算分组且升序排列后每个session包含的多条记录所对应的页面切片访问次数
    *
    * @param sortedAfterResult 升序排列后的当前分组的内容，Seq[Row]
    * @param pageSplitContainer 页面切片
    * @param tempResultContainer 临时结果的容器，打算存放各个页面切片中的访问次数，每个元素形如：1:1,1_2:1,1_2:1
    */
  def analysisNowGroupAfterSession(sortedAfterResult: Seq[Row], pageSplitContainer: ArrayBuffer[String], tempResultContainer: ArrayBuffer[(String, Int)]) = {
    //使用循环分析
    //待分析的：  //"page_flow":[1,2,3,4,5,6,7,8,9,99,80,100,98]
    // 找到当前session访问页面的轨迹，如：[1,2,3,98]
    //找到当前session访问页面的轨迹的页面切片，与待分析的页面切片依次进行比对，若吻合，将页面切片置于临时的结果容器中

    for (i <- 1 until sortedAfterResult.size) {
      val nowSessionPrePageId = sortedAfterResult(i - 1).getAs[Long]("page_id").toString
      val nowSessionCurrentPageId = sortedAfterResult(i).getAs[Long]("page_id").toString
      val nowSessionPageSplit = nowSessionPrePageId + "_" + nowSessionCurrentPageId

      //判断：若当前session访问页面的轨迹的首页的id与待分析的页面流的首页一致，需要进行累加
      if (i == 1) {
        if (nowSessionPrePageId.equals(pageSplitContainer(0))) {
          tempResultContainer.append((nowSessionPrePageId, 1))
        }
      }

      //当前若是与待分析的页面流的首页id吻合，也累计。情形：用户通过别的页面回跳到待分析的页面流的首页
      if (nowSessionCurrentPageId.equals(pageSplitContainer(0))) {
        tempResultContainer.append((nowSessionCurrentPageId, 1))
      }


      //与待分析的页面切片依次进行比对
      //1,1_2,2_3       3_2
      if (pageSplitContainer.contains(nowSessionPageSplit)) {
        tempResultContainer.append((nowSessionPageSplit, 1))
      }

    }
  }

  /**
    * 分析分组之后的结果，求得每个页面切片访问的次数
    *
    * @param groupAfterRDD ，每个元素形如：(sessionId,迭代器[Row,Row,Row,...])
    * @param bcPageSplitArr
    * @return RDD[(String, Int)],其中的每个元素形如：('1',1),('1_2',1)
    */
  def analysisGroupAfterResult(groupAfterRDD: RDD[(String, Iterable[Row])], bcPageSplitArr: Broadcast[ArrayBuffer[String]]): RDD[(String, Int)] ={
    //下述rdd中每个元素是对偶元组，key: 页面切片，1
    val resultRDD: RDD[(String, Int)] = groupAfterRDD.flatMap(perEle => {
      //临时的结果容器，存放：（"1_2",1）,（"1",1）,"2_3",1）,...
      val tempResultContainer: ArrayBuffer[(String, Int)] = new ArrayBuffer()

      //从广播变量中获取待分析的页面流页面切片容器
      val pageSplitContainer = bcPageSplitArr.value

      //每处理一次，就用来 分析一个聚合后的session
      val prepareSessionItr: Iterable[Row] = perEle._2

      //将分组后的每个session聚合后的结果根据action_time升序排列

      val sortedAfterResult: Seq[Row] = prepareSessionItr.toBuffer.
        sortWith(_.getAs[String]("action_time") < _.getAs[String]("action_time"))

      //分析当前分组且排序后的session
      analysisNowGroupAfterSession(sortedAfterResult, pageSplitContainer, tempResultContainer)

      tempResultContainer
    })

    //返回
    resultRDD
  }

  /**
    * 求得页面流的单跳转化率并落地到DB中
    *
    * @param resultArr ,每个元素形如：
    *                  (3_4,52)
    *                  (2_3,58)
    *                  (1,465)
    *                  (7_9,47)
    *                  (1_2,43)
    *                  (5_7,48)
    *                  (4_5,51)
    * @param pageSplitArr,每个元素形如：
    *                    1
    *                    1_2
    *                    2_3
    *                    ...
    * @param args ,存储的是参数task_id，如：2
    */
  def getPageSingleJumpRateAndSaveToDB(resultArr: Array[(String, Int)], pageSplitArr: ArrayBuffer[String], args: Array[String]) = {
    val container: Map[String, Int] = resultArr.toMap
    //println(container)

    // 分析Map，其中的结果形如：Map(8_9 -> 5, 3_4 -> 3, 2_3 -> 4, 9_99 -> 3, 4_5 -> 7, 6_7 -> 8, 1_2 -> 4, 1 -> 65, 7_8 -> 5, 5_6 -> 4, 99_80 -> 3)

    //用户存储最终的结果
    val finalResult: StringBuilder = new StringBuilder

    for (i <- 1 until pageSplitArr.size) {
      // 1
      val prePageSplit = pageSplitArr(i - 1)
      //1_2
      val currentPageSplit = pageSplitArr(i)

      //1 -> 65
      val prePageSplitAccessCnt = container.getOrElse(prePageSplit, 0)

      //1_2 -> 4
      val currentPageSplitAccessCnt = container.getOrElse(currentPageSplit, 0)

      var rate = 0.0

      if (prePageSplitAccessCnt != 0 && currentPageSplitAccessCnt != 0) {
        rate = NumberUtils.formatDouble(currentPageSplitAccessCnt.toDouble / prePageSplitAccessCnt, 5)
      }

      //使用容器将本次的结果存起来
      finalResult.append(currentPageSplit).append("=").append(rate).append("|")
    }

    //删除最后一个|
    finalResult.deleteCharAt(finalResult.length - 1)

    //println("最终结果是：\n"+finalResult)

    //将结果落地到db中
    saveToDB(args, finalResult.toString)

    //"page_flow":[1,2,3,4,5,6,7,8,9,99,80,100,98]}
    //最终结果是：
    //1_2=0.03509|2_3=2.0|3_4=0.5|4_5=2.0|5_6=0.0|6_7=0.0|7_8=1.33333|8_9=0.75|9_99=0.33333|99_80=3.0|80_100=0.0|100_98=0.0
  }


  /**
    * 将结果落地到db中
    *
    * @param args
    * @param resultStr
    */
  def saveToDB(args: Array[String], resultStr: String) = {
    val dao: IPageConvertRateDao = new PageConvertRateDaoImpl
    val bean: PageConvertRate = new PageConvertRate(args(0).toInt, resultStr)
    dao.insert(bean)
  }

  /**
    * 页面流对应的页面单跳转化率，并存储到db中
    *
    * @param result
    * @param args
    * @param spark
    */
  def calPageConvertRate(result: (RDD[Row], util.List[Integer]), args: Array[String], spark: SparkSession) = {
    //取出rdd，以及page_flow
    val rdd: RDD[Row] = result._1
    //"page_flow":[1,2,3,4,5,6,7,8,9,99,80,100,98]
    val page_flow: util.List[Integer] = result._2

    //1,将页面流切割成页面切片，存入到容器中
    val tuple: (ArrayBuffer[String], Broadcast[ArrayBuffer[String]]) = splitPageFlowAndSaveToBroacast(spark, page_flow)

    val pageSplitArr = tuple._1
    val bcPageSplitArr = tuple._2

    //2,分析rdd
    //前提：
    // 根据sessionid进行分组
    //rdd.foreach(println)
    //a)将rdd中每个元素转换成对偶元组
    //经过下一步后，RDD中每个元素形如：(session_id,row), 也就是：("14c99cba0d4944df804499484cb9d41f",[2019-01-05,34,14c99cba0d4944df804499484cb9d41f,9,2019-01-05 02:29:40,null,80,55,null,null,null,null])
    val tupleRDD: RDD[(String, Row)] = rdd.map(row => (row.getAs[String]("session_id"), row))

    //b)使用rdd的算子之groupByKey进行分组，目的是对分组后每个session进行分析，求页面切片访问数
    //("14c99cba0d4944df804499484cb9d41f",[2019-01-05,1,14c99cba0d4944df804499484cb9d41f,9,2019-01-05 02:29:20,null,80,55,null,null,null,null])
    //("14c99cba0d4944df804499484cb9d41f",[2019-01-05,3,14c99cba0d4944df804499484cb9d41f,9,2019-01-05 02:29:40,null,80,55,null,null,null,null])
    //("14c99cba0d4944df804499484cb9d41f",[2019-01-05,2,14c99cba0d4944df804499484cb9d41f,9,2019-01-05 02:29:50,null,80,55,null,null,null,null])

    val groupAfterRDD: RDD[(String, Iterable[Row])] = tupleRDD.groupByKey()



    //c)分析分组之后的结果
    val resultRDD: RDD[(String, Int)] = analysisGroupAfterResult(groupAfterRDD, bcPageSplitArr)

    //d)进行聚合，求得：各个页面切片访问数
    val resultArr: Array[(String, Int)] = resultRDD.reduceByKey(_ + _).collect()

    //resultArr.foreach(println)

    //e)求得页面流的单跳转化率并落地到DB中
    getPageSingleJumpRateAndSaveToDB(resultArr, pageSplitArr, args)
  }

  /**
    * 按条件筛选session
    *
    * @param args  入口数组，封装了job启动所必须的参数taskId
    * @param spark SparkSession
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
    val start_time = searchParam.getStart_time
    val end_time = searchParam.getEnd_time
    val page_flow = searchParam.getPage_flow

    //分析开始时间
    if (start_time != null && !start_time.isEmpty) {
      sqlBuffer.append(" and  (u.action_time >='").append(start_time).append("')")
    }
    //分析结束时间
    if (end_time != null && !end_time.isEmpty) {
      sqlBuffer.append(" and  (u.action_time <='").append(end_time).append("')")
    }

    //分析页面流
    if (page_flow != null && page_flow.size() > 0) {
      sqlBuffer.append(" and (u.page_id in").append(JSON.toJSONString(page_flow, SerializerFeature.UseSingleQuotes)
        .replace("[", "(").replace("]", ")")).append(")")
    }

   // spark.sql(sqlBuffer.toString).show(10000)

    //println(s"page_flow = $page_flow")

    //返回RDD
    (spark.sql(sqlBuffer.toString).rdd, page_flow)
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
}
