package com.merkey.entity.ad;

import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * Description：黑名单用户实体类<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author merkey
 * @version : 1.0
 */
@NoArgsConstructor
public class AdBlackList {
    private int user_id;

    public int getUser_id() {
        return user_id;
    }

    public void setUser_id(int user_id) {
        this.user_id = user_id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AdBlackList that = (AdBlackList) o;
        return user_id == that.user_id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(user_id);
    }

    public AdBlackList(int user_id) {
        this.user_id = user_id;
    }
}
