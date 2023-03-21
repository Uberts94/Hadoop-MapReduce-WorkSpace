package it.polito.bigdata.hadoop.lab;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/* This class is used to store a "userId" of type String, a score of type Double and
 * a product of type String
 *  */

public class RatingWritable implements Comparable<RatingWritable>, Writable {

	private String user; // userId
	private Double score; // score of the rating
	private String product; //productId

	public RatingWritable(String user, Double score, String product) {
		this.user = user;
		this.score = score;
		this.product = product;
	}

	public RatingWritable(RatingWritable other) {
		this.user = new String(other.getUser());
		this.score = Double.valueOf(other.getScore());
		this.product = new String(other.getProduct());
	}

	public RatingWritable() {
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public Double getScore() {
		return score;
	}

	public void setScore(Double score) {
		this.score = score;
	}
	
	public String getProduct() {
		return product;
	}
	
	public void setProduct(String product) {
		this.product = product;
	}

	@Override
	public int compareTo(RatingWritable other) {

		if (this.score.compareTo(other.getScore()) != 0) {
			return this.score.compareTo(other.getScore());
		} else { // if the count values of the two words are equal, the
					// lexicographical order is considered
			return this.score.compareTo(other.getScore());
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		user = in.readUTF();
		score = in.readDouble();
		product = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(user);
		out.writeDouble(score);
		out.writeUTF(product);
	}

	public String toString() {
		return new String(user + ":" + score);
	}

}