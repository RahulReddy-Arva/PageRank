/*
 Name: Rahul Reddy Arva
 ID: 800955965
 Title: Rank (Used for PageRank algorithm)

 */
package pagerank;


public class Rank {
	
	String title; //Used to store title
	double rank;  //Used to store rank
	
	public Rank(String title, double rank) {   // COnstructor
		super();
		this.title = title;
		this.rank = rank;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public double getRank() {
		return rank;
	}
	public void setRank(double rank) {
		this.rank = rank;
	}
	

}