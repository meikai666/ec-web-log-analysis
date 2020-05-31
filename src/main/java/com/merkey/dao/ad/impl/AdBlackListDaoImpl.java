package com.merkey.dao.ad.impl;

import com.merkey.dao.ad.IAdBlackListDao;
import com.merkey.entity.ad.AdBlackList;
import com.merkey.utils.DBCPUitl;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * Description：黑名单用户dao层接口实现类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date：2019年10月07日
 *
 * @author merkey
 * @version : 1.0
 */
public class AdBlackListDaoImpl implements IAdBlackListDao {
    private QueryRunner queryRunner;

    public AdBlackListDaoImpl() {
        queryRunner = new QueryRunner(DBCPUitl.getDataSource());
    }

    /**
     * @param beans 既包含历史的黑名单用户信息，也包含新拉黑的用户信息 (存储的是迄今为止所有的黑名单信息)
     */
    @Override
    public void insertBatchDealWith(List<AdBlackList> beans) {
        //前提：
        //①准备待批量新增的容器
        List<AdBlackList> prepareInsertContainer = new LinkedList<>();

        //思路：
        try {
            //①查询表中已经存在的数据 （user_id）
            String querySql = "select * from ad_blacklist";
            List<AdBlackList> oldBeansFromTable = queryRunner.query(querySql, new BeanListHandler<>(AdBlackList.class));

            //②将表中存在的数据依次与参数传入的所有的数据进行比对
            for (AdBlackList bean : beans) {
                if (!oldBeansFromTable.contains(bean)) {
                    //若不存在，将当前的Bean添加到待批量新增的容器中
                    prepareInsertContainer.add(bean);
                }
            }

            //③进行批量插入操作
            //批量插入
            String insertSql = "insert into ad_blacklist(user_id) values(?)";
            Object[][] batchInsertParmas = new Object[prepareInsertContainer.size()][];
            //需要修改数组中的元素值，必须使用普通for循环
            for (int i = 0; i < prepareInsertContainer.size(); i++) {
                AdBlackList bean = prepareInsertContainer.get(i);
                batchInsertParmas[i] = new Object[]{bean.getUser_id()};
            }
            queryRunner.batch(insertSql, batchInsertParmas);


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<AdBlackList> findAll() {
        try {
            return queryRunner.query("select * from ad_blacklist", new BeanListHandler<>(AdBlackList.class));
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("查询黑名单用户失败了哦！异常信息是：" + e.getMessage());
        }
    }
}
