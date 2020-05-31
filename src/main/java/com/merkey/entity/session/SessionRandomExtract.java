package com.merkey.entity.session;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Description：按时间比例随机抽取功能抽取出来的session实体类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
@Data
@NoArgsConstructor
public class SessionRandomExtract {
    /**
     * 任务编号
     */
    private int task_id;
    /**
     * session id
     */
    private String session_id;
    /**
     * session开始时间
     */
    private String start_time;

    /**
     * session结束时间
     */
    private String end_time;

    /**
     * 检索的所有关键字
     */
    private String search_keywords;

    /**
     * 点击了哪些品类的id
     */
    private String click_category_ids;


    public SessionRandomExtract(int task_id, String session_id, String start_time, String end_time, String search_keywords, String click_category_ids) {
        this.task_id = task_id;
        this.session_id = session_id;
        this.start_time = start_time;
        this.end_time = end_time;
        this.search_keywords = search_keywords;
        this.click_category_ids = click_category_ids;
    }
}
