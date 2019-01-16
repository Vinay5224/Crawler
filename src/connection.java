import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class connection {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	      // Creating Credentials 
	      MongoCredential credential = MongoCredential.createCredential("Provider", "ProviderMDM", 
	         "test123".toCharArray()); 
		 MongoClient mongo = new MongoClient( "192.168.0.2" , 27234 ); 

	      System.out.println("Connected to the database successfully");  
	      
	      // Accessing the database 
	      MongoDatabase database = mongo.getDatabase("ProviderMDM"); 

	      // Retrieving a collection
	      MongoCollection<Document> collection = database.getCollection("sampleCollection"); 
	      System.out.println("Collection sampleCollection selected successfully");

	      Document document = new Document("title", "MongoDB") 
	      .append("id", 1)
	      .append("description", "database") 
	      .append("likes", 100) 
	      .append("url", "http://www.tutorialspoint.com/mongodb/") 
	      .append("by", "tutorials point");  
	      collection.insertOne(document);
	      System.out.println("Document inserted successfully");    

	}

}
