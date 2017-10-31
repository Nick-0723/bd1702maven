package com.scl.mapreduce.join.reduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@SuppressWarnings("all")
public class IdTag implements WritableComparable<IdTag> {
    private IntWritable id;
    private DoubleWritable tag;

    IdTag(){
        this.tag = new DoubleWritable();
        this.id = new IntWritable();
    }

    public IdTag(int id, double tag) {
        this.tag = new DoubleWritable(tag);
        this.id = new IntWritable(id);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id.get());
        out.writeDouble(this.tag.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id.set(in.readInt());
        this.tag.set(in.readDouble());
    }

    public IntWritable getId() {
        return id;
    }

    public DoubleWritable getTag() {
        return tag;
    }

    @Override
    public String toString() {
        return id + "\t" + tag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IdTag that = (IdTag) o;

        if (getId() != null ? !getId().equals(that.getId()) : that.getId() != null) return false;
        if (getTag() != null ? !getTag().equals(that.getTag()) : that.getTag() != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = getId() != null ? getId().hashCode() : 0;
        result = 31 * result + (getTag() != null ? getTag().hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(IdTag o) {
        if(o==null)return 1;
        if(this.id.compareTo(o.id)==0)return this.tag.compareTo(o.tag);
        else return this.id.compareTo(o.id);
    }
}
