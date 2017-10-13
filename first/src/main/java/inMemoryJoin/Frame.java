package inMemoryJoin;

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
    String userid;
    double rating;

    @Override
    public int compareTo(Frame other) {
        return userid.compareTo(other.userid);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.id);
        dataOutput.writeUTF(this.userid);
        dataOutput.writeUTF(String.valueOf(this.rating));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.userid = dataInput.readUTF();
        this.rating = Double.parseDouble(dataInput.readUTF());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Frame frame = (Frame) o;

        return userid != null ? userid.equals(frame.userid) : frame.userid == null;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = id != null ? id.hashCode() : 0;
        result = 31 * result + (userid != null ? userid.hashCode() : 0);
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

    public void setUserId(String userid) {
        this.userid = userid;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    public String getId() {
        return this.id;
    }

    public String getUserId() {
        return this.userid;
    }

    public Double getRating() {
        return this.rating;
    }


    public String toString() {
        return this.userid;
    }
}
