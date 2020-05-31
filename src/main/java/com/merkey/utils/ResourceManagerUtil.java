package com.merkey.utils;


import com.merkey.common.CommonConstant;
import com.merkey.common.RunMode;

import java.io.IOException;
import java.util.Properties;

/**
 * Description：资源文件操作工具类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年10月27日
 *
 * @author merkey
 * @version : 1.0
 */
public class ResourceManagerUtil {
    /**
     * 资源文件操作的集合类
     */
    private static Properties properties;


    /**
     * job运行模式
     */
    public static RunMode runMode;

    static {
        properties = new Properties();

        try {
            properties.load(ResourceManagerUtil.class.getClassLoader().getResourceAsStream(CommonConstant.COMMON_CONFIG_FILE_NAME));

            runMode = RunMode.valueOf(properties.getProperty(CommonConstant.SPARK_JOB_RUN_MODE).toUpperCase());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据传入的key获得资源文件中对应的value
     *
     * @param key
     * @return
     */
    public static String getPropertiesValueByKey(String key) {
        return properties.getProperty(key);
    }

}
