package com.merkey.entity.common;

/**
 * Description：Task（任务）实体类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
public class Task {
    /**
     * 表的主键
     */
    private int task_id;
    /**
     * 任务名称
     */
    private String task_name;
    /**
     * 创建时间
     */
    private String create_time;
    /**
     * 开始运行的时间
     */
    private String start_time;
    /**
     * 结束运行的时间
     */
    private String finish_time;
    /**
     * 任务类型，就是说，在一套大数据平台中，肯定会有各种不同类型的统计分析任务，比
     * 如说用户访问session分析任务，页面单跳转化率统计任务；所以这个字段就标识了每个
     *  任务的类型
     */
    private String task_type;

    /**
     * 任务状态，任务对应的就是一次Spark作业的运行，这里就标识了，Spark作业是新建，
     * 还没运行，还是正在运行，还是已经运行完毕
     */
    private String task_status;
    /**
     * 最最重要，用来使用JSON的格式，来封装用户提交的任务对应的特殊的筛选参数
     */
    private String task_param;

    public int getTask_id() {
        return task_id;
    }

    public void setTask_id(int task_id) {
        this.task_id = task_id;
    }

    public String getTask_name() {
        return task_name;
    }

    public void setTask_name(String task_name) {
        this.task_name = task_name;
    }

    public String getCreate_time() {
        return create_time;
    }

    public void setCreate_time(String create_time) {
        this.create_time = create_time;
    }

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public String getFinish_time() {
        return finish_time;
    }

    public void setFinish_time(String finish_time) {
        this.finish_time = finish_time;
    }

    public String getTask_type() {
        return task_type;
    }

    public void setTask_type(String task_type) {
        this.task_type = task_type;
    }

    public String getTask_status() {
        return task_status;
    }

    public void setTask_status(String task_status) {
        this.task_status = task_status;
    }

    public String getTask_param() {
        return task_param;
    }

    public void setTask_param(String task_param) {
        this.task_param = task_param;
    }
}
