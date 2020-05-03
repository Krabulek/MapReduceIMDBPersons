import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class SumJobs implements WritableComparable<SumJobs> {
    IntWritable numDirected;
    IntWritable numPlayed;

    public SumJobs() {
        set(new IntWritable(0), new IntWritable(0));
    }

    public void set(IntWritable numDirected, IntWritable numPlayed) {
        this.numDirected = numDirected;
        this.numPlayed = numPlayed;
    }

    public IntWritable getNumDirected() {
        return numDirected;
    }

    public IntWritable getNumPlayed() {
        return numPlayed;
    }

    public void addSumJobs(SumJobs sumJobs) {
        set(new IntWritable(this.numDirected.get() + sumJobs.getNumDirected().get()),
                new IntWritable(this.numPlayed.get() + sumJobs.getNumPlayed().get()));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        numDirected.write(dataOutput);
        numPlayed.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        numDirected.readFields(dataInput);
        numPlayed.readFields(dataInput);
    }

    @Override
    public int compareTo(SumJobs sumJobs) {
        int comparison = numDirected.compareTo(sumJobs.numDirected);
        if (comparison != 0) {
            return comparison;
        }
        return numPlayed.compareTo(sumJobs.numPlayed);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SumJobs sumJobs = (SumJobs) o;
        return numDirected.equals(sumJobs.numDirected) &&
                numPlayed.equals(sumJobs.numPlayed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numDirected, numPlayed);
    }

    @Override
    public String toString() {
        return numDirected + "\t" + numPlayed;
    }
}
