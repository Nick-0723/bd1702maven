package com.scl.utils;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@SuppressWarnings("all")
public class UserGoodsTag implements WritableComparable<UserGoodsTag> {
    private Text user_goods;
    private DoubleWritable tag;

    UserGoodsTag(){
        this.tag = new DoubleWritable();
        this.user_goods = new Text();
    }

    public UserGoodsTag(String user_goods, double tag) {
        this.tag = new DoubleWritable(tag);
        this.user_goods = new Text(user_goods);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.user_goods.write(out);
        this.tag.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.user_goods.readFields(in);
        this.tag.readFields(in);
    }

    public Text getUser_goods() {
        return user_goods;
    }

    public DoubleWritable getTag() {
        return tag;
    }

    @Override
    public String toString() {
        return user_goods + "\t" + tag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UserGoodsTag that = (UserGoodsTag) o;

        if (getUser_goods() != null ? !getUser_goods().equals(that.getUser_goods()) : that.getUser_goods() != null) return false;
        if (getTag() != null ? !getTag().equals(that.getTag()) : that.getTag() != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = getUser_goods() != null ? getUser_goods().hashCode() : 0;
        result = 31 * result + (getTag() != null ? getTag().hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(UserGoodsTag o) {
        if(o==null)return 1;
        if(this.user_goods.compareTo(o.user_goods)==0)return this.tag.compareTo(o.tag);
        else return this.user_goods.compareTo(o.user_goods);
    }
}
