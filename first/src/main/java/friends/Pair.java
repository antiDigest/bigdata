package friends;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Utility class solving the problem of pairs as a key
 */
public class Pair implements Writable, WritableComparable<Pair> {

    String first;
    String second;

    public Pair() {
    }

    public Pair(String first, String second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(Pair friend) {
        if ((friend.first.equals(this.first) && friend.second.equals(this.second))
                || (friend.first.equals(this.second) && friend.second.equals(this.first)))
            return 0;
        else if ((this.first.compareTo(friend.first) < 0)
                || (this.first.compareTo(friend.first) == 0 && this.second.compareTo(friend.second) < 0))
            return -1;
        else return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair pair = (Pair) o;

        if (first != null ? !first.equals(pair.first) : pair.first != null) return false;
        return second != null ? second.equals(pair.second) : pair.second == null;
    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + (second != null ? second.hashCode() : 0);
        return result;
    }

    public void readFields(DataInput input) throws IOException {
        this.first = input.readUTF();
        this.second = input.readUTF();
    }

    public void write(DataOutput output) throws IOException {
        output.writeUTF(this.first);
        output.writeUTF(this.second);
    }

    public String toString() {
        return this.first + "," + this.second;
    }

    /**
     * HELPER FUNCTIONS
     */
    public void setFirst(String person) {
        this.first = person;
    }

    public void setSecond(String person) {
        if (Integer.parseInt(person) < (Integer.parseInt(this.first))) {
            this.second = this.first;
            this.first = person;
        } else
            this.second = person;
    }

    public String getFirst() {
        return this.first;
    }

    public String getSecond() {
        return this.second;
    }
}