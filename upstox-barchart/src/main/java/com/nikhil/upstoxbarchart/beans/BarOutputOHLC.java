package com.nikhil.upstoxbarchart.beans;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BarOutputOHLC {
    private String event = "ohlc_notify";
    private String symbol;
    private Integer bar_num;
    private Double o;
    private Double h;
    private Double l;
    private Double c;
    private Double volume;

}
