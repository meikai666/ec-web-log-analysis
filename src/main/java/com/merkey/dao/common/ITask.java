package com.merkey.dao.common;

import com.merkey.entity.common.Task;

/**
 * Description：dao层之task 任务 <br/>
 * Copyright (c) ，2019 ， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 * Date： 2019年12月25日
 *
 * @author merkey
 * @version : 1.0
 */
public interface ITask {
    /**
     * 根据任务编号查询任务详情
     *
     * @param task_id
     * @return
     */
    Task findTaskById(int task_id);
}
