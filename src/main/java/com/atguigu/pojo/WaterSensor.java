package com.atguigu.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Administrator
 * @date 2022/3/3 11:09
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    public String id;
    public Long ts;
    public Integer vc;

    public WaterSensor(String id, Integer vc) {
        this.id = id;
        this.vc = vc;
    }
}
