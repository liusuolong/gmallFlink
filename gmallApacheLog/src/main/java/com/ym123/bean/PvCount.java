package com.ym123.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ymstart
 * @create 2020-11-26 15:31
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PvCount {

    private String pv;
    private Long windowEnd;
    private Long count;

}