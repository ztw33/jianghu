package cn.nju.st13.preprocess;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordPair implements WritableComparable<WordPair> {
    private String word1;
    private String word2;

    WordPair() {
        word1 = "";
        word2 = "";
    }

    WordPair(String word1, String word2) {
        this.word1 = word1;
        this.word2 = word2;
    }

    @Override
    public int hashCode() {
        return (word1.hashCode()+word2.hashCode())*17;
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof WordPair))
            return false;
        WordPair w = (WordPair)o;
        return (this.word1.equals(w.word1) && (this.word2.equals(w.word2))) || (this.word1.equals(w.word2) && this.word2.equals(w.word1));
    }

    @Override
    public int compareTo(WordPair o) {
        String t1 = (this.word1.compareTo(this.word2) < 0) ? this.word1+this.word2 : this.word2+this.word1;
        String t2 = (o.word1.compareTo(o.word2) < 0) ? o.word1+o.word2 : o.word2+o.word1;
        return t1.compareTo(t2);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word1);
        dataOutput.writeUTF(word2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word1 = dataInput.readUTF();
        this.word2 = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return word1+","+word2;
    }
}
