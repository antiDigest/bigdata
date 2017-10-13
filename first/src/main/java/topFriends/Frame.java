package topFriends;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Frame to sort using count
 */
public class Frame implements Writable, WritableComparable<Frame> {

    String first;
    String second;
    int count;

    @Override
    public int compareTo(Frame other) {
        if (other.count == this.count)
            return 0;
        else if (other.count < this.count)
            return 1;
        else
            return -1;
    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + (second != null ? second.hashCode() : 0);
        result = 31 * result + count;
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Frame frame = (Frame) o;

        if (count != frame.count) return false;
        if (first != null ? !first.equals(frame.first) : frame.first != null) return false;
        return second != null ? second.equals(frame.second) : frame.second == null;
    }

    /**
     * HELPER FUNCTIONS
     */
    public void setFirst(String person) {
        this.first = person;
    }

    public void setSecond(String person) {
        if (person.compareTo(this.first) < 0) {
            this.second = this.first;
            this.first = person;
        } else
            this.second = person;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getFirst() {
        return this.first;
    }

    public String getSecond() {
        return this.second;
    }

    public Integer getCount() {
        return this.count;
    }

    public void readFields(DataInput input) throws IOException {
        this.first = input.readUTF();
        this.second = input.readUTF();
        this.count = Integer.parseInt(input.readUTF());
    }

    public void write(DataOutput output) throws IOException {
        output.writeUTF(this.first);
        output.writeUTF(this.second);
        output.writeUTF(String.valueOf(this.count));
    }

    public String toString() {
        return this.first + "," + this.second;
    }
}
