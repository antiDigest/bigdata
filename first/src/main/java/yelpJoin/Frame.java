package yelpJoin;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Frame to sort using count
 */
public class Frame implements Writable, WritableComparable<Frame> {

    String id;
    String address;
    String list;
    double rating;

    @Override
    public int compareTo(Frame other) {
        if (other.rating == this.rating)
            return 0;
        else if (other.rating < this.rating)
            return -1;
        else
            return 1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.id);
        dataOutput.writeUTF(this.address);
        dataOutput.writeUTF(this.list);
        dataOutput.writeUTF(String.valueOf(this.rating));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.address = dataInput.readUTF();
        this.list = dataInput.readUTF();
        this.rating = Double.parseDouble(dataInput.readUTF());
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = id != null ? id.hashCode() : 0;
        result = 31 * result + (address != null ? address.hashCode() : 0);
        result = 31 * result + (list != null ? list.hashCode() : 0);
        temp = Double.doubleToLongBits(rating);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    /**
     * HELPER FUNCTIONS
     */
    public void setId(String id) {
        this.id = id;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void setList(String list) {
        this.list = list;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    public String getId() {
        return this.id;
    }

    public String getAddress() {
        return this.address;
    }

    public String getList() {
        return this.list;
    }

    public Double getRating() {
        return this.rating;
    }


    public String toString() {
        return this.id + "\t" + this.address + "\t" + this.list;
    }
}
