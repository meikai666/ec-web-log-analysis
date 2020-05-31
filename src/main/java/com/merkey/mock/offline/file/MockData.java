package com.merkey.mock.offline.file;


import com.merkey.utils.DateUtils;
import com.merkey.utils.StringUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Description：模拟日志文件产生的数据<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public class MockData {
    private static Random random;

    private static String[] searchKeywords;
    //模拟的是用户的具体行为 （检索，点击，下单，支付）
    private static String[] actions;

    //模拟电商平台注册用户的昵称
    private static String[] nickNames;
    //模拟电商平台注册用户的真实姓名（真实项目中需要进行数据脱敏，如：张xx）
    private static String[] userNames;
    //模拟电商平台注册用户的职业
    private static String[] professionals;
    //模拟电商平台注册用户的所在地
    private static String[] cities;
    /**
     * 模拟的是注册用户浏览了电商品台上哪些型号的电子产品
     */
    private static String[] productNames;

    //所点击的产品id容器，key:产品id，value：产品id所属的分类id
    private static Map<Long, Long> clickProductIdMap;

    //所下单的产品id容器，key:下单产品id，value：下单产品id所属的下单分类id
    private static Map<String, String> orderProductIdsMap;

    static {
        random = new Random();

        //模拟商城的用户在搜索框键入的检索关键字
        searchKeywords = new String[]{"小米", "iPhone", "魅族", "荣耀", "OPPO", "vivo", "华为", "三星", "智能手环", "游戏手柄"};

        //模拟的是用户的具体行为 （检索，点击，下单，支付），同一时刻在页面上，只有一个行为
        actions = new String[]{"search", "click", "order", "pay"};

        //模拟电商平台注册用户的昵称
        nickNames = new String[]{"Emma", "Mary", "Allen", "Olivia", "Natasha", "Kevin", "Rose", "Kelly", "Jeanne", "James", "Edith", "Sophia", "Charles", "Ashley", "William", "Hale", "Steve", "David", "Richard", "Daniel", "Matthew", "Mark", "Andrew", "Jean", "Vera", "John", "Tracy", "Shirley", "Grace", "Gary", "Ruth", "Robert", "Hannah", "Beverly", "Angel", "Christopher", "Viola", "Maria", "Evelyn", "Lucy", "Gloria", "Amy", "Sylvia", "Timothy", "Sharon", "Van", "Sandy", "Ruby", "Rachel", "Judy", "Diana", "Jessie", "Jason", "June", "Julie", "Carol", "Gina", "Lois", "Diane", "Ella", "Donna", "Julia", "Alice", "Beth", "Sandra", "Bonnie", "Ann", "Alma", "Helen", "Denise", "Frances", "Betty", "Theresa", "Pamela", "Elaine", "Dorothy", "Kathy", "Kathleen", "Ellen", "Lisa", "Christina", "Vicky", "Virginia", "Sherry", "Linda"};

        //模拟电商平台注册用户的真实姓名
        userNames = new String[]{"宋爱梅", "王志芳", "于光", "贾隽仙", "贾燕青", "刘振杰", "郭卫东", "崔红宇", "马福平", "冯红", "崔敬伟", "穆增志", "谢志威", "吕金起", "韩云庆", "鲁全福", "郭建立", "郝连水", "闫智胜", "辰梓", "邦星", "祯桓", "腾枫", "晨祜", "翰吉", "骏喆", "勇栋", "逸卓", "家禧", "然震", "枫杞", "骏骏", "华胤", "晨运", "福桀", "胤寅", "运俊", "凡德", "锦骞", "骞晨  泽骏", "暄家", "芃然", "骏翱", "休振", "谛爵", "嘉轩", "国中", "栋升", "振翱", "振骏", "文文", "寅骞", "逸运", "远芃", "驰嘉", "权邦", "休宇", "桓运", "运驰", "运潍  礼腾", "中祯", "运辰", "祯初", "天凯", "震栋", "驰振", "辰泽", "晨谛", "哲轩", "子荣", "文龙", "然振", "暄骏", "裕运", "裕腾", "梓栋", "辰振", "国仕", "炳禧", "嘉材  骏星", "运骏", "辞轩", "杞寅", "帆运", "楷祥", "晨骏", "骏峰", "哲锐", "锟琛", "家嘉", "骞家", "禧鹏", "喆锋", "晓天", "枫梓", "骏家"};

        //模拟电商平台注册用户的职业
        // professionals = new String[]{"教师", "工人", "记者", "演员", "厨师", "医生", "护士", "司机", "军人", "律师", "商人", "会计", "店员", "出纳", "作家", "导游", "模特", "警察", "歌手", "画家", "裁缝", "翻译", "法官", "保安", "花匠", "服务员", "清洁工", "建筑师", "理发师", "采购员", "设计师", "消防员", "机修工", "推销员", "魔术师", "模特儿", "邮递员", "售货员", "救生员", "运动员", "工程师", "飞行员", "管理员", "机械师", "经纪人", "审计员", "漫画家", "园艺师", "科学家", "主持人"};
        professionals = new String[]{"教师", "工人", "记者", "演员", "厨师", "医生", "护士", "司机", "军人", "律师"};

        //模拟电商平台注册用户的所在地
        //cities = new String[]{"南京", "无锡", "徐州", "常州", "苏州", "南通", "连云港", "淮安", "盐城", "扬州", "镇江", "泰州", "宿迁", "杭州", "宁波", "温州", "嘉兴", "湖州", "绍兴", "金华", "衢州", "舟山", "台州", "丽水", "合肥", "芜湖", "蚌埠", "淮南", "马鞍山", "淮北", "铜陵", "安庆", "黄山", "滁州", "阜阳", "宿州", "巢湖", "六安", "亳州", "池州", "宣城", "南昌", "景德镇", "萍乡", "九江", "新余", "鹰潭", "赣州", "吉安", "宜春", "抚州", "上饶", "合肥", "芜湖", "蚌埠", "淮南", "马鞍山", "淮北", "铜陵", "安庆", "黄山", "滁州", "阜阳", "宿州", "巢湖", "六安", "亳州", "池州", "宣城", "石家庄", "唐山", "秦皇岛", "邯郸", "邢台", "保定", "张家口", "承德", "沧州", "廊坊", "衡水", "北京", "上海", "天津", "重庆", "哈尔滨", "齐齐哈尔", "鸡西", "鹤岗", "双鸭山", "大庆", "伊春", "佳木斯", "七台河", "牡丹江", "黑河", "绥化", "大兴安岭"};
        cities = new String[]{"南京", "无锡", "徐州", "常州", "苏州", "南通", "连云港", "淮安", "盐城", "扬州"};
        //cities = new String[]{"石家庄", "唐山", "秦皇岛", "沈阳", "鞍山", "本溪", "南京", "无锡", "常州", "杭州", "宁波", "温州"};

        /**
         * 模拟的是注册用户浏览了电商品台上哪些型号的电子产品
         */
        productNames = new String[]{"Xiaomi/小米 小米手机6", "Apple/苹果 iPhone X", "Meizu/魅族 魅蓝 S6", "honor/荣耀 荣耀V10",
                "OPPO R11", "vivo X20", "Huawei/华为 Mate 10 6G+", "Samsung/三星 Galaxy S9+", "Xiaomi/小米 小米手环2", "Microsoft/微软  Xbox one 手柄"};


        //所点击的产品id容器，key:产品id，value：产品id所属的分类id
        clickProductIdMap = new LinkedHashMap<>();
        //所下单的产品id容器，key:下单产品id，value：下单产品id所属的下单分类id
        orderProductIdsMap = new LinkedHashMap<>();
    }

    public static void main(String[] args) throws Exception {
        mock();
    }


    /**
     * 模拟数据
     */
    public static void mock() throws Exception {

        //模拟用户访问电商平台的动作
        mockUserAccessAction();

        //模拟用户的注册信息
        mockUserRegistInfos();

        //模拟产品信息
        mockProductInfos();
    }

    /**
     * 模拟产品信息
     */
    private static void mockProductInfos() throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter("data/product_info.log"));

        StringBuffer buffer = new StringBuffer();

        //0,1表示产品的两种状态，如：自营或是第三方
        int[] productStatus = new int[]{0, 1};
        for (int i = 0; i < 100; i++) {
            long productId = i;
            String productName = productNames[random.nextInt(productNames.length)];
            //json对象格式的字符串
            String extendInfo = "{\"product_status\": " + productStatus[random.nextInt(productStatus.length)] + "}";

            buffer.append(productId).append("|")
                    .append(productName).append("|")
                    .append(extendInfo);

            bw.write(buffer.toString());
            bw.newLine();

            buffer.delete(0, buffer.length());
        }

        bw.close();
    }

    /**
     * 模拟用户在电商平台上的注册信息 (用来向hive表中user_info存入数据)
     */
    private static void mockUserRegistInfos() throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter("data/user_info.log"));

        StringBuilder builder = new StringBuilder();

        String[] sexes = new String[]{"男", "女"};
        for (int i = 0; i < 100; i++) {
            long userid = i;
            String username = nickNames[random.nextInt(nickNames.length)];
            String name = userNames[random.nextInt(userNames.length)];
            int age = random.nextInt(50) + 18;
            String professional = professionals[random.nextInt(professionals.length)];
            String sex = sexes[random.nextInt(sexes.length)];
            String city = cities[random.nextInt(cities.length)];


            builder.append(userid).append("|")
                    .append(username).append("|")
                    .append(name).append("|")
                    .append(age).append("|")
                    .append(professional).append("|")
                    .append(sex).append("|")
                    .append(city);

            bw.write(builder.toString());
            bw.newLine();

            builder.delete(0, builder.length());
        }

        bw.close();
    }

    /**
     * 模拟用户访问电商平台的动作，用来向hive表user_visit_action中导入数据
     *
     * @throws IOException
     */
    private static void mockUserAccessAction() throws IOException {
        String date = DateUtils.getTodayDate();
        BufferedWriter bw = new BufferedWriter(new FileWriter("data/user_visit_action.log"));

        //每天有一百个用户访问电商品台
        for (int i = 0; i < 100; i++) {
            long userId = random.nextInt(100);

            //每个用户每天访问十次电商平台(注意：指的是10个session，不是10个页面！！)
            for (int j = 0; j < 10; j++) {
                //UUID:生成全球唯一的一个字符串标识符
                String sessionId = UUID.randomUUID().toString().replace("-", "");

                //每次会话随机访问页面
                randomAccessPage(date, bw, userId, sessionId);
            }
        }

        bw.close();
    }

    /**
     * 一个用户每次会话期间随机访问页面
     *
     * @param date
     * @param bw
     * @param userId
     * @param sessionId
     * @throws IOException
     */
    private static void randomAccessPage(String date, BufferedWriter bw, long userId, String sessionId) throws IOException {

        //字符串构建器，用于临时存储每一条日志信息
        StringBuffer buffer = new StringBuffer();


        String baseActionTime = date + " " + StringUtils.fulfuill(String.valueOf(random.nextInt(24)));
        //每次会话期间一个用户访问1~100个页面
        //每循环一次，构建一个Row的实例(就是一条日志信息)，该实例中的数据最终用来作为hive表user_visit_action中的一条记录

        //每循环一次，模拟的是一个用户在一次session范围内，随机访问的页面的动作（点击，搜索，下单，支付等等）
        int maxAccessPageCnt = 1 + random.nextInt(99);

        FLG:
        for (int k = 0; k < maxAccessPageCnt; k++) {//random.nextInt(99)→【0,99】+1→【1,100】
            //【0,100)
            long pageId = random.nextInt(100);//每循环一次，只能访问一个页面。页面编号与页面数没有对应关系的！
            String actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(60))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(60)));


            String searchKeyword = null;
            Long clickProductId = null;
            String orderCategoryIds = null;
            String orderProductIds = null;
            String payCategoryIds = null;
            String payProductIds = null;
            Long clickCategoryId = null;

            //随机获取一个动作
            String action = actions[random.nextInt(actions.length)];
            if ("search".equals(action)) {
                searchKeyword = searchKeywords[random.nextInt(searchKeywords.length)];
            } else if ("click".equals(action)) {//优化之后，保证一个产品同时只能属于一个类别
                //给产品id设置值之前，必须先判断对应的分类id是否有值，若没有，需要设置。因为产品一定是属于某个分类的！！
                clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));

                clickCategoryId = clickProductIdMap.getOrDefault(clickProductId, Long.valueOf(String.valueOf(random.nextInt(100))));

                //建立映射关系（下述的map中，key:产品id，value: 产品对应的分类的id）
                clickProductIdMap.put(clickProductId, clickCategoryId);
            } else if ("order".equals(action)) {//下单的产品的id的集合是点击产品id集合的子集，同理，下单的产品类别的id的集合是点击产品类别id集合的子集
                //旧
                // orderCategoryIds = String.valueOf(random.nextInt(100));
                // orderProductIds = String.valueOf(random.nextInt(100));

                //拦截非法的操作 （若用户没有点击任意的产品，何来下单？）
                if (clickProductIdMap.size() == 0) {
                    k--;
                    continue FLG;
                }

                //更新后：
                Set<Long> allClickProductIds = clickProductIdMap.keySet();
                Long[] allClickProductIdsArr = allClickProductIds.toArray(new Long[allClickProductIds.size()]);

                Long randomProductId = allClickProductIdsArr[random.nextInt(allClickProductIds.size())];
                orderProductIds = String.valueOf(randomProductId);//orderProductIds→表示当前的产品属于哪个订单的，订单产品id就是产品id
                orderCategoryIds = String.valueOf(clickProductIdMap.get(randomProductId));//当前产品分类的id就是订单包含产品分类的id

//                if (orderProductIds == null || orderCategoryIds == null) {
//                    continue FLG;
//                }

                //也就是：订单包含产品的id的集合一定是产品id集合的子集；同理，订单分类的id的集合也一定是产品分类id集合的子集
                orderProductIdsMap.put(orderProductIds, orderCategoryIds);
            } else if ("pay".equals(action)) {//支付的产品的id的集合是下单（添加购物车）产品id集合的子集，同理，支付的产品类别的id的集合是下单产品类别id集合的子集
                //旧
                //payCategoryIds = String.valueOf(random.nextInt(100));
                //payProductIds = String.valueOf(random.nextInt(100));


                //拦截非法的操作（购物车若是空的，何来支付？）
                if (orderProductIdsMap.size() == 0) {
                    k--;
                    continue FLG;
                }

                //更新后：
                Set<String> allOrderProductIds = orderProductIdsMap.keySet();
                String[] allOrderProductIdsArr = allOrderProductIds.toArray(new String[allOrderProductIds.size()]);
                String randomOrderProductId = allOrderProductIdsArr[random.nextInt(allOrderProductIds.size())];

                payProductIds = randomOrderProductId;
                payCategoryIds = orderProductIdsMap.get(randomOrderProductId);

//                if (payProductIds == null || payCategoryIds == null) {
//                    continue FLG;
//                }
            }

            //将产生的日志信息添加到StringBuffer中
            buffer.append(date).append("|")
                    .append(userId).append("|")
                    .append(sessionId).append("|")
                    .append(pageId).append("|")
                    .append(actionTime).append("|")
                    .append(searchKeyword).append("|")
                    .append(clickCategoryId).append("|")
                    .append(clickProductId).append("|")
                    .append(orderCategoryIds).append("|")
                    .append(orderProductIds).append("|")
                    .append(payCategoryIds).append("|")
                    .append(payProductIds);

            bw.write(buffer.toString());
            bw.newLine();

            //清空buffer
            buffer.delete(0, buffer.length());
        }
    }
}
