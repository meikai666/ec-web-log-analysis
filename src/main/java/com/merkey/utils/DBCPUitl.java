package com.merkey.utils;

import com.merkey.common.CommonConstant;
import org.apache.commons.dbcp.BasicDataSourceFactory;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Description：DBCP连接池工具类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年10月27日
 *
 * @author merkey
 * @version : 1.0
 */
public class DBCPUitl {

    private static Properties properties;

    /**
     * 连接池的实例
     */
    private static DataSource ds;

    static {
        properties = new Properties();
        //根据job的运行环境，获得对应的目录
        String runMode = ResourceManagerUtil.runMode.name().toLowerCase();
        try {

            String path = runMode + File.separator + CommonConstant.DBCP_CONN_FILE_NAME;
            properties.load(DBCPUitl.class.getClassLoader().getResourceAsStream(path));

            ds = BasicDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获得连接池的实例
     *
     * @return
     */
    public static DataSource getDataSource() {
        return ds;
    }


    /**
     * 开启事务
     *
     * @param conn
     */
    public static void beginTrasaction(Connection conn) throws SQLException {
            conn.setAutoCommit(false);
    }

    /**
     * 提交事务
     *
     * @param conn
     */
    public static void commitTrasaction(Connection conn) throws SQLException {
        conn.commit();
    }

    /**
     * 回滚事务
     *
     * @param conn
     */
    public static void rollbackTrasaction(Connection conn)  {
        try {
            conn.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * 资源释放
     *
     * @param conn
     */
    public static void releaseResource(Connection conn) {
        if(conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 从连接池中获得连接的实例
     *
     * @return
     */
    public static Connection getConnection() {
        try {
            return ds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
