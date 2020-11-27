package com.ym123.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ymstart
 * @create 2020-11-25 19:05
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ItemCount {
    private Long itemId;
    private Long windowEnd;
    private Long count;
}
