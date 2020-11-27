package com.ym123.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ymstart
 * @create 2020-11-26 11:41
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApacheLogCount {
    private String url;
    private Long windowEnd;
    private Long urlCount;
}
