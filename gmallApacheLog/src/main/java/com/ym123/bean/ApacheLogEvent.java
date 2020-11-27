package com.ym123.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ymstart
 * @create 2020-11-26 11:16
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApacheLogEvent {
    private String ip;
    private String useId;
    private Long eventTime;
    private String method;
    private String url;
}
