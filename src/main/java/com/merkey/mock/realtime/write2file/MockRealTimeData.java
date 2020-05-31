package com.merkey.mock.realtime.write2file;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


/**
 * Description：模拟实时产生的数据 （测试或者是生产环境下，模拟用户点击电商平台上的广告，实时将广告的日志信息写入文件的情形  ）<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public class MockRealTimeData extends Thread {

    /**
     * 广告日志文件名
     */
    private String adLogfileName;

    private BufferedWriter bw;

    private Random random = new Random();
    private String[] provinces = new String[]{"河北", "辽宁", "江苏", "浙江", "山东"};
    private Map<String, String[]> provinceCityMap = new HashMap<String, String[]>();


    public MockRealTimeData() {
        provinceCityMap.put("河北", new String[]{"石家庄", "唐山", "秦皇岛", "邯郸", "邢台", "保定", "张家口", "承德", "沧州", "廊坊", "衡水"});
        provinceCityMap.put("辽宁", new String[]{"沈阳", "大连", "鞍山", "抚顺", "本溪", "丹东", "锦州", "营口", "阜新", "辽阳", "盘锦", "铁岭", "朝阳", "葫芦岛"});
        provinceCityMap.put("江苏", new String[]{"南京", "无锡", "徐州", "常州", "苏州", "南通", "连云港", "淮安", "盐城", "扬州", "镇江", "泰州", "宿迁"});
        provinceCityMap.put("浙江", new String[]{"杭州", "宁波", "温州", "嘉兴", "湖州", "绍兴", "金华", "衢州", "舟山", "台州", "丽水"});
        provinceCityMap.put("山东", new String[]{"济南", "青岛", "淄博", "枣庄", "东营", "烟台", "潍坊", "威海", "济宁", "泰安", "日照", "莱芜", "临沂", "德州"});
    }


    public MockRealTimeData(String adLogfileName) {
        this();
        this.adLogfileName = adLogfileName;
        //BufferedWriter
        try {
            bw = new BufferedWriter(new FileWriter(adLogfileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        //每循环一次，实时模拟某个省份某个城市下的一个用户点击某一个广告的情形
        while (true) {
            String province = provinces[random.nextInt(provinces.length)];
            String[] cities = provinceCityMap.get(province);
            String city = cities[random.nextInt(cities.length)];
            //log：消息的组成，系统当前时间 省份 城市 用户id 广告id
            String log = System.currentTimeMillis() + " " + province + " " + city + " "
                    + random.nextInt(100) + " " + random.nextInt(10);
            try {
                //向文件中写入广告日志信息
                bw.write(log);
                bw.newLine();
                bw.flush();
                Thread.sleep(1500);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 启动Kafka Producer
     *
     * @param args
     */
    public static void main(String[] args) {
        //接收传入的广告日志文件名
        String adLogfileName = args[0].trim();

        MockRealTimeData producer = new MockRealTimeData(adLogfileName);
        producer.start();
    }

}
