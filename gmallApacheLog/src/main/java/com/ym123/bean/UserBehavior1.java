package com.ym123.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ymstart
 * @create 2020-11-25 13:52
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserBehavior1 {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;
}
