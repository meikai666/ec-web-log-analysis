package com.merkey.dao.goods.impl;

import com.merkey.dao.goods.ICityInfoDao;
import com.merkey.entity.goods.CityInfo;
import com.merkey.utils.DBCPUitl;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import java.sql.SQLException;
import java.util.List;

/**
 * Description：城市信息dao层接口实现类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public class CityInfoDaoImpl implements ICityInfoDao {
    private QueryRunner queryRunner;

    public CityInfoDaoImpl() {
        queryRunner = new QueryRunner(DBCPUitl.getDataSource());
    }

    @Override
    public List<CityInfo> findAll() {
        try {
            //注意：若是表的字段与实体类的属性不一致，没法使用DBUtils框架，无法将记录封装到实例相应的属性中。
            //如何应对？ select时，给相应的字段添加别名，别名与实体类的属性一样，就可以解决这样的问题！！
            return queryRunner.query("select city_id cityId,city_name cityName,area from city_info", new BeanListHandler<>(CityInfo.class));
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("查询城市信息失败了哦！异常信息是：" + e.getMessage());
        }
    }
}
