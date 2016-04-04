package com.elixir.hadoop.Chromo;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//need clarity
public class SecondrySort {
	public static class IntPair 
    implements WritableComparable<IntPair> {
		
		private int PrimaryKey=0;
		private int SecondryKey=0;
		public int getPrimaryKey() {
			return PrimaryKey;
		}
		
		public void setPrimaryKey(int primaryKey) {
			PrimaryKey = primaryKey;
		}
		
		public int getSecondryKey() {
			return SecondryKey;
		}
		
		public void setSecondryKey(int secondryKey) {
			SecondryKey = secondryKey;
		}

		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeInt(PrimaryKey - Integer.MIN_VALUE);
		      out.writeInt(SecondryKey - Integer.MIN_VALUE);
		}
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			PrimaryKey = in.readInt()+Integer.MIN_VALUE;
			SecondryKey = in.readInt()+Integer.MIN_VALUE;
		}
		public int compareTo(IntPair arg0) {
			// TODO Auto-generated method stub
			 if (PrimaryKey != arg0.PrimaryKey) {
		        return PrimaryKey < arg0.PrimaryKey ? -1 : 1;
		      } else if (SecondryKey != arg0.SecondryKey) {
		        return SecondryKey < arg0.SecondryKey ? -1 : 1;
		      } else {
		        return 0;
		      }
		    }

		public void set(int ChrNum, int i) {
			PrimaryKey=ChrNum;
			SecondryKey=i;
			// TODO Auto-generated method stub
			}
	}
	public static class Comparator extends WritableComparator {
	      public Comparator() {
	        super(IntPair.class);
	      }

	      public int compare(byte[] b1, int s1, int l1,
	                         byte[] b2, int s2, int l2) {
	        return compareBytes(b1, s1, l1, b2, s2, l2);
	      }
	    }
	static {                                        // register this comparator
	      WritableComparator.define(IntPair.class, new Comparator());
	    }
	 public static class FirstGroupingComparator 
     implements RawComparator<IntPair> {
		 
			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8, 
			                                  b2, s2, Integer.SIZE/8);
			}
			
			public int compare(IntPair o1, IntPair o2) {
			int l = o1.getPrimaryKey();
			int r = o2.getSecondryKey();
			return l == r ? 0 : (l < r ? -1 : 1);
			}
	 }
}
