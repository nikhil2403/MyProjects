package com.nikhil.upstoxbarchart.beans;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.time.Instant;


@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BarOHLC implements Comparable<BarOHLC> {
    private String sym;
    @JsonProperty(value = "P")
    private Double P;
    @JsonProperty(value = "Q")
    private Double Q;
   // @JsonProperty(value = "TS2")
    private Instant TS2;
    @JsonProperty(value = "TS2")
    private Long TS;

    public Instant getTS2() {
        return Instant.ofEpochSecond(0L,getTS());
    }

    @Override
    public int compareTo(BarOHLC o) {
        return this.getTS2().compareTo(o.getTS2());
    }
}
