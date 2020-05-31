package com.merkey.offline.goods

import com.alibaba.fastjson.JSON
import com.merkey.dao.common.impl.ITaskDaoImpl
import com.merkey.dao.goods.{ICityInfoDao, IProductAreaTop3Dao}
import com.merkey.dao.goods.impl.{CityInfoDaoImpl, ProductAreaTop3DaoImpl}
import com.merkey.entity.common.{SearchParam, Task}
import com.merkey.entity.goods.{CityInfo, ProductAreaTop3, ProductStatus}
import com.merkey.mock.offline.memory.MockData
import com.merkey.utils.ResourceManagerUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Description：热门商品离线统计<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年12月26日  
  *
  * @author merkey
  * @version : 1.0
  */
object HotGoodsCalApplication {


  def main(args: Array[String]): Unit = {

    //步骤
    //前提：
    val spark: SparkSession = commonOperate(args)

    //1,按条件筛选session
    filterSessionByCondition(args, spark)


    //2，将mysql中的表city_info装载到内存中，映射为一张虚拟表
    loadCityTableToMemory(spark)

    //3,三张虚拟表进行关联查询操作,求得待分析的基础数据（内连接）
    generateBaseData(spark)

    //4,分析基础数据，求top3，并保存到db中
    anylysBaseDataAndSaveToDB(spark, args)



    //资源释放
    spark.close()

  }


  /**
    * 分析基础数据，求top3，并保存到db中
    *
    * @param spark
    * @param args
    */
  def anylysBaseDataAndSaveToDB(spark: SparkSession, args: Array[String]) = {
    //MySQL中，设计结果表，task_id、area、area_level、product_id、city_names、
    // click_count、product_name、product_status
    val rdd:RDD[Row] = spark.sql(
      """
        |select
        |  area,
        |  area_level,
        |  product_id,
        |  city_names,
        |  click_count,
        |  product_name,
        |  product_status,
        |  row_number() over(distribute by area sort by click_count desc) flg
        |from(
        |    select
        |        area,
        |        max(area_level) area_level,
        |        product_id,
        |        concat_ws(',',collect_set(cityName)) city_names,
        |        count(product_id) click_count,
        |        max(product_name) product_name,
        |        max(product_status) product_status
        |    from temp_hot_goods
        |    group by area,product_id
        |) t having  flg<=3
      """.stripMargin)
      .rdd


    //保存到db中
    //将task_id置于广播变量中，分发到每个Executor进程中
    val task_id = args(0).toInt
    val bcTaskId = spark.sparkContext.broadcast(task_id)

    rdd.repartition(1).foreachPartition(itr => {
      if (!itr.isEmpty) {
        val productDao: IProductAreaTop3Dao = new ProductAreaTop3DaoImpl
        val beans: java.util.List[ProductAreaTop3] = new java.util.LinkedList[ProductAreaTop3]

        itr.foreach(row => {
          val task_id = bcTaskId.value
          val area = row.getAs[String]("area")
          val area_level = row.getAs[String]("area_level")
          val product_id = row.getAs[Long]("product_id").toInt
          val product_name = row.getAs[String]("product_name")
          val click_count = row.getAs[Long]("click_count").toInt
          val product_status = row.getAs[String]("product_status")
          val city_names = row.getAs[String]("city_names")

          val bean: ProductAreaTop3 = new ProductAreaTop3(task_id: Int, area: String, area_level: String, product_id: Int, product_name: String, click_count: Int, product_status: String, city_names: String)
          beans.add(bean)
        })

        productDao.insertBatch(beans)
      }
    })
  }

  /**
    *
    * 自定义函数，根据传入的区域的名词，获得对应的级别
    *
    * 根据地区名求得对应的级别
    *
    * @param area
    */
  def getAreaLevel(area: String) = {
    var areaLevel: String = ""
    //华东大区，A级，华中大区，B级，东北大区，C级，西北大区，D级
    area match {
      case "华东大区" => areaLevel = "A级"
      case "华中大区" => areaLevel = "B级"
      case "东北大区" => areaLevel = "C级"
      case "西北大区" => areaLevel = "D级"
      case _ => areaLevel = "E级"
    }

    //返回
    areaLevel
  }

  /**
    * 根据json对象格式的数据，获得flg，返回其对应的业务含义，如：0→自营；1→第三方
    *
    * @param productStatus
    */
  def getProductStatus(productStatus: String) = {
    //解析JSON对象格式的数据，求出product_status
    val bean: ProductStatus = JSON.parseObject(productStatus, classOf[ProductStatus])
    val status = bean.getProduct_status

    //根据值获得真实的业务意思，反馈   0~>自营；1~>第三方
    if (status == 0) "自营" else "第三方"
  }

  /**
    * 三张虚拟表进行关联查询操作,求得待分析的基础数据（内连接）
    *
    * @param spark
    */
  def generateBaseData(spark: SparkSession) = {
    //①自定义一个函数，根据地区名求得对应的级别
    spark.udf.register("getAreaLevel", (area: String) => getAreaLevel(area))

    //②自定义一个函数，根据产品的状态求出真实的信息，如：{"product_status": 1} ~> "第三方"
    spark.udf.register("getProductStatus", (productStatus: String) => getProductStatus(productStatus))


    //③关联查询 (hive表可以和虚拟表一起操作)
    val sql =
      """
        | select
        |            c.area,
        |            getAreaLevel(c.area) area_level,
        |            p.product_id,
        |            p.product_name,
        |            getProductStatus( p.extend_info) product_status,
        |            c.cityName
        |  from filter_after_visit_action v, city_info c,web_log_analysis_hive.product_info p
        |  where v. city=c.cityName  and
        |             v.click_product_id = p.product_id
      """.stripMargin

    //spark.sql(sql).show(10000)
    spark.sql(sql).createOrReplaceTempView("temp_hot_goods")
    spark.sqlContext.cacheTable("temp_hot_goods")
  }

  /**
    *
    * 将mysql中的表city_info装载到内存中，映射为一张虚拟表
    * 原因：
    * 内存中的虚拟表和mysql中的物理表完全不同！
    *
    * @param spark
    */
  private def loadCityTableToMemory(spark: SparkSession) = {
    val dao: ICityInfoDao = new CityInfoDaoImpl
    val cityBeans: java.util.List[CityInfo] = dao.findAll()
    spark.createDataFrame(cityBeans, classOf[CityInfo]).createOrReplaceTempView("city_info")
    //    spark.sql("select * from city_info").show(1000)
    spark.sqlContext.cacheTable("city_info")
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
    sqlBuffer.append("select  u.click_product_id, i.city  from web_log_analysis_hive.user_visit_action u, web_log_analysis_hive.user_info i where u.user_id=i.user_id and  u.click_product_id is not null ")
    val start_time = searchParam.getStart_time
    val end_time = searchParam.getEnd_time

    //分析开始时间
    if (start_time != null && !start_time.isEmpty) {
      sqlBuffer.append(" and  (u.action_time >='").append(start_time).append("')")
    }
    //分析结束时间
    if (end_time != null && !end_time.isEmpty) {
      sqlBuffer.append(" and  (u.action_time <='").append(end_time).append("')")
    }


    //将临时表映射为虚拟表
    spark.sql(sqlBuffer.toString).createOrReplaceTempView("filter_after_visit_action")

    //缓存虚拟表
    spark.sqlContext.cacheTable("filter_after_visit_action")
  }

  /**
    * 共通的操作
    *
    * @param args
    * @return SparkSession
    */
  def commonOperate(args: Array[String]): SparkSession = {
    //Spark高版本中，核心类：SparkSession,封装了SpardConf, SparkContext,SQLContext
    val builder = SparkSession.builder().appName(this.getClass.getSimpleName)

    //判断当前job运行的模式，若是local，手动设置；若是test，production模式，通过spark-submit --master xx
    if ("local".equals(ResourceManagerUtil.runMode.name().toLowerCase())) {
      builder.master("local[*]")
    }
    val spark: SparkSession = builder.enableHiveSupport().getOrCreate()

    //拦截非法的操作
    if (args == null || args.length != 1) {
      print("请传入TaskId!")
      System.exit(-1)
    }

    //设置日志的级别
    spark.sparkContext.setLogLevel("WARN")

    //返回
    spark
  }
}
