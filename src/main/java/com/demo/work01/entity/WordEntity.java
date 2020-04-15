package com.demo.work01.entity;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author affable
 * @description 单词实体
 * @date 2020/4/15 17:19
 */
@Data
public class WordEntity implements WritableComparable<WordEntity> {

    private String word;

    private int count;

    @Override
    public int compareTo(WordEntity o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
