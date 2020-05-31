package com.merkey.entity.session;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Description：存储top10每个品类的点击top10的session的实体类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
@Data
@NoArgsConstructor
public class Top10CategorySession {
    private int task_id;
    private int category_id;
    private String session_id;
    private int click_count;

    public Top10CategorySession(int task_id, int category_id, String session_id, int click_count) {
        this.task_id = task_id;
        this.category_id = category_id;
        this.session_id = session_id;
        this.click_count = click_count;
    }
}
