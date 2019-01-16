/*import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class ReadingFrmHadoop {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkSession spark = SparkSession.builder().appName("Reading_CSV_File").master("local").getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark);
		SQLContext sql = new SQLContext(jsc);

		
		Dataset<Row> ss = spark.read().csv("hdfs://192.168.0.108:8020/user/exa4/SS.csv");
		ss.show();
		
		Dataset<Row> sa = spark.
		
	}

}
*/


import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;
import org.json.JSONException;
import org.json.JSONObject;

import com.github.wnameless.json.flattener.JsonFlattener;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;

import scala.collection.JavaConversions;


import org.apache.log4j.Logger;
import org.apache.parquet.Strings;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.log4j.Level;
public class ProviderMDM {

	static Properties crawlingProperties;
	static String DB, host, port, mongoDBUsername, mongoDBPassword,  originalCollection, notificationCollection, trackHistory, driverMemory, webUIparameters,  dbParameters, thread, lim;
	static String user,Updatednpi;
	static String zipfilepath = "/home/ubuntu/Documents/CMSZip/";

	static int ThreadPoolCount,Limit;
	static ArrayList<String> webParam,dbParam ;// = Splitting(webUIparameters);

	static String DBUnderscore = "NPI,Entity_Type_Code, Replacement_NPI,Employer_Identification_Number_EIN,Provider_Organization_Name_Legal_Business_Name,Provider_Last_Name_Legal_Name ,Provider_First_Name,Provider_Middle_Name  , Provider_Name_Prefix_Text ,Provider_Name_Suffix_Text,Provider_Credential_Text,Provider_Other_Organization_Name,Provider_Other_Organization_Name_Type_Code,Provider_Other_Last_Name,Provider_Other_First_Name,Provider_Other_Middle_Name,Provider_Other_Name_Prefix_Text,Provider_Other_Name_Suffix_Text,Provider_Other_Credential_Text ,Provider_Other_Last_Name_Type_Code,Provider_First_Line_Business_Mailing_Address, Provider_Second_Line_Business_Mailing_Address,Provider_Business_Mailing_Address_City_Name,Provider_Business_Mailing_Address_State_Name,Provider_Business_Mailing_Address_Postal_Code,Provider_Business_Mailing_Address_Country_Code_If_outside_US ,Provider_Business_Mailing_Address_Telephone_Number,Provider_Business_Mailing_Address_Fax_Number ,Provider_First_Line_Business_Practice_Location_Address,Provider_Second_Line_Business_Practice_Location_Address,Provider_Business_Practice_Location_Address_City_Name,Provider_Business_Practice_Location_Address_State_Name,Provider_Business_Practice_Location_Address_Postal_Code,Provider_Business_Practice_Location_Address_Country_Code_If_outside_US,Provider_Business_Practice_Location_Address_Telephone_Number,Provider_Business_Practice_Location_Address_Fax_Number,Provider_Enumeration_Date,Last_Update_Date,NPI_Deactivation_Reason_Code,NPI_Deactivation_Date,NPI_Reactivation_Date,Provider_Gender_Code,Authorized_Official_Last_Name,Authorized_Official_First_Name,Authorized_Official_Middle_Name,Authorized_Official_Title_or_Position,Authorized_Official_Telephone_Number,Healthcare_Provider_Taxonomy_Code_1,Provider_License_Number_1,Provider_License_Number_State_Code_1,Healthcare_Provider_Primary_Taxonomy_Switch_1,Healthcare_Provider_Taxonomy_Code_2,Provider_License_Number_2,Provider_License_Number_State_Code_2,Healthcare_Provider_Primary_Taxonomy_Switch_2,Healthcare_Provider_Taxonomy_Code_3,Provider_License_Number_3,Provider_License_Number_State_Code_3,Healthcare_Provider_Primary_Taxonomy_Switch_3,Healthcare_Provider_Taxonomy_Code_4,Provider_License_Number_4,Provider_License_Number_State_Code_4,Healthcare_Provider_Primary_Taxonomy_Switch_4,Healthcare_Provider_Taxonomy_Code_5,Provider_License_Number_5,Provider_License_Number_State_Code_5,Healthcare_Provider_Primary_Taxonomy_Switch_5,Healthcare_Provider_Taxonomy_Code_6,Provider_License_Number_6,Provider_License_Number_State_Code_6,Healthcare_Provider_Primary_Taxonomy_Switch_6,Healthcare_Provider_Taxonomy_Code_7,Provider_License_Number_7,Provider_License_Number_State_Code_7,Healthcare_Provider_Primary_Taxonomy_Switch_7,Healthcare_Provider_Taxonomy_Code_8,Provider_License_Number_8,Provider_License_Number_State_Code_8,Healthcare_Provider_Primary_Taxonomy_Switch_8,Healthcare_Provider_Taxonomy_Code_9,Provider_License_Number_9,Provider_License_Number_State_Code_9,Healthcare_Provider_Primary_Taxonomy_Switch_9,Healthcare_Provider_Taxonomy_Code_10,Provider_License_Number_10,Provider_License_Number_State_Code_10,Healthcare_Provider_Primary_Taxonomy_Switch_10,Healthcare_Provider_Taxonomy_Code_11,Provider_License_Number_11,Provider_License_Number_State_Code_11,Healthcare_Provider_Primary_Taxonomy_Switch_11,Healthcare_Provider_Taxonomy_Code_12,Provider_License_Number_12,Provider_License_Number_State_Code_12,Healthcare_Provider_Primary_Taxonomy_Switch_12,Healthcare_Provider_Taxonomy_Code_13,Provider_License_Number_13  ,Provider_License_Number_State_Code_13  ,Healthcare_Provider_Primary_Taxonomy_Switch_13  ,		    Healthcare_Provider_Taxonomy_Code_14  ,		    Provider_License_Number_14  ,		    Provider_License_Number_State_Code_14  ,		    Healthcare_Provider_Primary_Taxonomy_Switch_14  ,		    Healthcare_Provider_Taxonomy_Code_15  ,		    Provider_License_Number_15  ,		    Provider_License_Number_State_Code_15  , Healthcare_Provider_Primary_Taxonomy_Switch_15  ,		    Other_Provider_Identifier_1 ,		    Other_Provider_Identifier_Type_Code_1 ,		    Other_Provider_Identifier_State_1,		    Other_Provider_Identifier_Issuer_1  ,		    Other_Provider_Identifier_2  ,		    Other_Provider_Identifier_Type_Code_2  ,		    Other_Provider_Identifier_State_2  ,		    Other_Provider_Identifier_Issuer_2  ,		    Other_Provider_Identifier_3  ,		    Other_Provider_Identifier_Type_Code_3  ,		    Other_Provider_Identifier_State_3  ,		    Other_Provider_Identifier_Issuer_3  ,		    Other_Provider_Identifier_4  ,		    Other_Provider_Identifier_Type_Code_4  ,		    Other_Provider_Identifier_State_4  ,		    Other_Provider_Identifier_Issuer_4  ,		    Other_Provider_Identifier_5  ,		    Other_Provider_Identifier_Type_Code_5  ,		    Other_Provider_Identifier_State_5  ,		    Other_Provider_Identifier_Issuer_5  ,		    Other_Provider_Identifier_6  ,		    Other_Provider_Identifier_Type_Code_6  ,		    Other_Provider_Identifier_State_6  ,		    Other_Provider_Identifier_Issuer_6  ,		    Other_Provider_Identifier_7  ,		    Other_Provider_Identifier_Type_Code_7  ,		    Other_Provider_Identifier_State_7  ,		    Other_Provider_Identifier_Issuer_7  ,		    Other_Provider_Identifier_8  ,		    Other_Provider_Identifier_Type_Code_8  ,		    Other_Provider_Identifier_State_8  ,		    Other_Provider_Identifier_Issuer_8  ,		    Other_Provider_Identifier_9  ,		    Other_Provider_Identifier_Type_Code_9  ,		    Other_Provider_Identifier_State_9  ,		    Other_Provider_Identifier_Issuer_9  ,		    Other_Provider_Identifier_10  ,		    Other_Provider_Identifier_Type_Code_10  ,		    Other_Provider_Identifier_State_10  ,		    Other_Provider_Identifier_Issuer_10  ,		    Other_Provider_Identifier_11  ,		    Other_Provider_Identifier_Type_Code_11  ,		    Other_Provider_Identifier_State_11  ,		    Other_Provider_Identifier_Issuer_11  ,		    Other_Provider_Identifier_12  ,		    Other_Provider_Identifier_Type_Code_12  ,		    Other_Provider_Identifier_State_12  ,		    Other_Provider_Identifier_Issuer_12  ,		    Other_Provider_Identifier_13  ,		    Other_Provider_Identifier_Type_Code_13  ,		    Other_Provider_Identifier_State_13  ,		    Other_Provider_Identifier_Issuer_13  ,		    Other_Provider_Identifier_14  ,		    Other_Provider_Identifier_Type_Code_14  ,		    Other_Provider_Identifier_State_14  ,		    Other_Provider_Identifier_Issuer_14  ,		    Other_Provider_Identifier_15  ,		    Other_Provider_Identifier_Type_Code_15  ,		    Other_Provider_Identifier_State_15  ,		    Other_Provider_Identifier_Issuer_15  ,		    Other_Provider_Identifier_16  ,		    Other_Provider_Identifier_Type_Code_16  ,		    Other_Provider_Identifier_State_16  ,		    Other_Provider_Identifier_Issuer_16  ,		    Other_Provider_Identifier_17  ,		    Other_Provider_Identifier_Type_Code_17  ,		    Other_Provider_Identifier_State_17  ,		    Other_Provider_Identifier_Issuer_17  ,		    Other_Provider_Identifier_18  ,		    Other_Provider_Identifier_Type_Code_18  ,		    Other_Provider_Identifier_State_18  ,		    Other_Provider_Identifier_Issuer_18  ,		    Other_Provider_Identifier_19  ,		    Other_Provider_Identifier_Type_Code_19  ,		    Other_Provider_Identifier_State_19  ,		    Other_Provider_Identifier_Issuer_19  ,		    Other_Provider_Identifier_20  ,		    Other_Provider_Identifier_Type_Code_20  ,		    Other_Provider_Identifier_State_20  ,		    Other_Provider_Identifier_Issuer_20  ,		    Other_Provider_Identifier_21  ,		    Other_Provider_Identifier_Type_Code_21  ,		    Other_Provider_Identifier_State_21  ,		    Other_Provider_Identifier_Issuer_21  ,		    Other_Provider_Identifier_22  ,		    Other_Provider_Identifier_Type_Code_22  ,		    Other_Provider_Identifier_State_22  ,		    Other_Provider_Identifier_Issuer_22  ,		    Other_Provider_Identifier_23  ,		    Other_Provider_Identifier_Type_Code_23  ,		    Other_Provider_Identifier_State_23  ,		    Other_Provider_Identifier_Issuer_23  ,		    Other_Provider_Identifier_24  ,		    Other_Provider_Identifier_Type_Code_24  ,		    Other_Provider_Identifier_State_24  ,		    Other_Provider_Identifier_Issuer_24  ,		    Other_Provider_Identifier_25  ,		    Other_Provider_Identifier_Type_Code_25  ,		    Other_Provider_Identifier_State_25  ,		    Other_Provider_Identifier_Issuer_25  ,		    Other_Provider_Identifier_26  ,		    Other_Provider_Identifier_Type_Code_26  ,		    Other_Provider_Identifier_State_26  ,		    Other_Provider_Identifier_Issuer_26  ,		    Other_Provider_Identifier_27  ,		    Other_Provider_Identifier_Type_Code_27  ,		    Other_Provider_Identifier_State_27  ,		    Other_Provider_Identifier_Issuer_27  ,		    Other_Provider_Identifier_28  ,		    Other_Provider_Identifier_Type_Code_28  ,		    Other_Provider_Identifier_State_28  ,		    Other_Provider_Identifier_Issuer_28  ,Other_Provider_Identifier_29  ,Other_Provider_Identifier_Type_Code_29  ,Other_Provider_Identifier_State_29  ,Other_Provider_Identifier_Issuer_29  ,		    Other_Provider_Identifier_30  ,		    Other_Provider_Identifier_Type_Code_30  ,		    Other_Provider_Identifier_State_30  ,		    Other_Provider_Identifier_Issuer_30  ,		    Other_Provider_Identifier_31  ,		    Other_Provider_Identifier_Type_Code_31  ,		    Other_Provider_Identifier_State_31  ,		    Other_Provider_Identifier_Issuer_31  ,Other_Provider_Identifier_32  ,		    Other_Provider_Identifier_Type_Code_32  ,		    Other_Provider_Identifier_State_32  ,		    Other_Provider_Identifier_Issuer_32  ,		    Other_Provider_Identifier_33  ,		    Other_Provider_Identifier_Type_Code_33  ,		    Other_Provider_Identifier_State_33  ,		    Other_Provider_Identifier_Issuer_33  ,		    Other_Provider_Identifier_34  ,		    Other_Provider_Identifier_Type_Code_34  ,		    Other_Provider_Identifier_State_34  ,		    Other_Provider_Identifier_Issuer_34  ,		    Other_Provider_Identifier_35  ,		    Other_Provider_Identifier_Type_Code_35  ,		    Other_Provider_Identifier_State_35  ,		    Other_Provider_Identifier_Issuer_35,		    Other_Provider_Identifier_36,Other_Provider_Identifier_Type_Code_36,Other_Provider_Identifier_State_36,Other_Provider_Identifier_Issuer_36,Other_Provider_Identifier_37,Other_Provider_Identifier_Type_Code_37,Other_Provider_Identifier_State_37,Other_Provider_Identifier_Issuer_37,Other_Provider_Identifier_38,Other_Provider_Identifier_Type_Code_38,Other_Provider_Identifier_State_38,Other_Provider_Identifier_Issuer_38,Other_Provider_Identifier_39,Other_Provider_Identifier_Type_Code_39,Other_Provider_Identifier_State_39,Other_Provider_Identifier_Issuer_39,Other_Provider_Identifier_40,Other_Provider_Identifier_Type_Code_40,Other_Provider_Identifier_State_40,Other_Provider_Identifier_Issuer_40,Other_Provider_Identifier_41,Other_Provider_Identifier_Type_Code_41,Other_Provider_Identifier_State_41,Other_Provider_Identifier_Issuer_41,Other_Provider_Identifier_42,Other_Provider_Identifier_Type_Code_42,Other_Provider_Identifier_State_42,Other_Provider_Identifier_Issuer_42,Other_Provider_Identifier_43,Other_Provider_Identifier_Type_Code_43,Other_Provider_Identifier_State_43,Other_Provider_Identifier_Issuer_43,Other_Provider_Identifier_44,Other_Provider_Identifier_Type_Code_44,Other_Provider_Identifier_State_44,Other_Provider_Identifier_Issuer_44,Other_Provider_Identifier_45,Other_Provider_Identifier_Type_Code_45,Other_Provider_Identifier_State_45,Other_Provider_Identifier_Issuer_45,Other_Provider_Identifier_46,Other_Provider_Identifier_Type_Code_46,Other_Provider_Identifier_State_46,Other_Provider_Identifier_Issuer_46,Other_Provider_Identifier_47,Other_Provider_Identifier_Type_Code_47,Other_Provider_Identifier_State_47,Other_Provider_Identifier_Issuer_47,Other_Provider_Identifier_48,Other_Provider_Identifier_Type_Code_48,Other_Provider_Identifier_State_48,Other_Provider_Identifier_Issuer_48,Other_Provider_Identifier_49,Other_Provider_Identifier_Type_Code_49,Other_Provider_Identifier_State_49,Other_Provider_Identifier_Issuer_49,Other_Provider_Identifier_50,Other_Provider_Identifier_Type_Code_50,Other_Provider_Identifier_State_50,Other_Provider_Identifier_Issuer_50,Is_Sole_Proprietor,Is_Organization_Subpart,Parent_Organization_LBN,Parent_Organization_TIN,Authorized_Official_Name_Prefix_Text  , Authorized_Official_Name_Suffix_Text,Authorized_Official_Credential_Text,Healthcare_Provider_Taxonomy_Group_1,Healthcare_Provider_Taxonomy_Group_2,Healthcare_Provider_Taxonomy_Group_3,Healthcare_Provider_Taxonomy_Group_4,Healthcare_Provider_Taxonomy_Group_5,Healthcare_Provider_Taxonomy_Group_6,Healthcare_Provider_Taxonomy_Group_7,Healthcare_Provider_Taxonomy_Group_8,Healthcare_Provider_Taxonomy_Group_9,Healthcare_Provider_Taxonomy_Group_10,Healthcare_Provider_Taxonomy_Group_11,Healthcare_Provider_Taxonomy_Group_12,Healthcare_Provider_Taxonomy_Group_13,Healthcare_Provider_Taxonomy_Group_14,Healthcare_Provider_Taxonomy_Group_15";



	//static MongoClient client = new MongoClient("localhost", 27017);
	static MongoDatabase database; //= client.getDatabase("Testing");
	static MongoCollection<Document> collection; //= database.getCollection("WholeCollection");
	static MongoCollection<Document> collectionNotify; //= database.getCollection("notification");
	static MongoCollection<Document> collectionTrackNpi; //= database.getCollection("Track");updatednpiCollection
	static  MongoCollection<Document> collectionUpdatednpi;

	public static void main(String[] args) throws IOException, JSONException, InterruptedException {

		System.out.println("Staring Application JsonFilesUpdatedNPI1 Shutdown shutdownNow await 3h");
		 user=args[0];

		List<StructField> fields = new ArrayList<>();

		ArrayList<String> a_sschema_fields = SplitParam(DBUnderscore);

		for(String a:a_sschema_fields){

			fields.add(DataTypes.createStructField( a, DataTypes.StringType, true)); 
		}

		StructType customSchema = DataTypes.createStructType(fields);



		//Initializing the parameters from the config file 
		InputStream filename=new FileInputStream("/home/"+user+"/Documents/Crawling.properties");
		crawlingProperties=new Properties();
		crawlingProperties.load(filename);
		DB = crawlingProperties.getProperty("MongoDBDatabase").trim();
		host = crawlingProperties.getProperty("MongoDBHost").trim();
		port = crawlingProperties.getProperty("MongoDBPort").trim();
		mongoDBUsername = crawlingProperties.getProperty("MongoDBUsername").trim();
		mongoDBPassword = crawlingProperties.getProperty("MongoDBPassword").trim();

		originalCollection = crawlingProperties.getProperty("OriginalCollection").trim();
		notificationCollection = crawlingProperties.getProperty("NotificationCollection").trim();
		trackHistory = crawlingProperties.getProperty("TrackHistory").trim();
		Updatednpi="Updatednpi";
		 
		//driverMemory = crawlingProperties.getProperty("DriverMemory").trim();
		webUIparameters = crawlingProperties.getProperty("WebUIparameters").trim();
		dbParameters = crawlingProperties.getProperty("DBParameters").trim(); 
		thread = crawlingProperties.getProperty("ThreadPoolCount").trim();
		ThreadPoolCount = Integer.parseInt(thread);
		lim = crawlingProperties.getProperty("Limit").trim();
		Limit = Integer.parseInt(lim);


		//Spitting the array 
		webParam = SplitParam(webUIparameters);
		dbParam = SplitParam(dbParameters);

		//Database 
		//MongoCredential credential = MongoCredential.createCredential(mongoDBUsername, DB, mongoDBPassword.toCharArray());
		//MongoClient client = new MongoClient(new ServerAddress("192.168.0.230", 27234), Arrays.asList(credential));

		MongoClient client = new MongoClient(new ServerAddress(host,Integer.parseInt(port)));
		database = client.getDatabase(DB);
		collection = database.getCollection(originalCollection);
		collectionNotify = database.getCollection(notificationCollection);
		
		collectionTrackNpi=database.getCollection(trackHistory);
		collectionUpdatednpi= database.getCollection(Updatednpi);

		 

		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		String mst ="local[*]";
		//String mst ="spark://192.168.0.108:7077";
		SparkConf conf = new SparkConf().setAppName("cust data").setMaster(mst);
		SparkSession spark = SparkSession
				.builder()
				.config(conf)
				//.config("spark.mongodb.input.uri", "mongodb://"+MONGO_USR_NM+":"+MONGO_USR_PS+"@"+IP+":"+PORT+"/"+DB+".T")
				.config("spark.mongodb.input.uri", "mongodb://"+host+":"+port+"/"+DB+".T")
				.config("spark.mongodb.output.uri","mongodb://"+host+":"+port+"/"+DB+".T")
				/* .config("spark.driver.memory", driverMemory)*/
				/*.config("spark.sql.autoBroadcastJoinThreshold", "-1")*/
				.config("spark.exeuctor.extraJavaOptions","-XX:+UseG1GC")
				.getOrCreate();

		
		 
		 for(int i = 0; i<4; i++){
			 long startTime = System.currentTimeMillis();
			 logicCode(spark,customSchema);
			long endTime = System.currentTimeMillis();
			System.out.println("That took " + (endTime - startTime) + " milliseconds");
		 }


	}






	public static void logicCode(SparkSession spark,StructType customSchema) throws InterruptedException, IOException, JSONException {

		String Usernm=user;
	 
		String jsonpathUpdatedNPI = "/home/"+user+"/Documents/JsonFilesUpdatedNPI1";
		 
		
		//delet folder 
		Arrays.stream(new File(jsonpathUpdatedNPI).listFiles()).forEach(File::delete);
		
		ExecutorService executor = Executors.newFixedThreadPool(ThreadPoolCount);
		FindIterable<Document> search = collection.find().limit(Limit).noCursorTimeout(true);
		Iterator<Document> Iter = search.iterator();
		Date stdate = new Date();
		System.out.println("Download Partial NPI LUD start "+stdate.toString());
		while(Iter.hasNext()){
			Document doc = Iter.next();
				
			if((doc.get("NPI_Deactivation_Date").equals("")) && (doc.get("NPI_Reactivation_Date").equals("")))
					executor.submit(new MyCallable(doc.getString("NPI"),doc.getString("Last_Update_Date")) );
		 
		} 


	     executor.shutdown();
	     executor.awaitTermination(3,TimeUnit.HOURS); 
	     
	     int countofFiles = new File(jsonpathUpdatedNPI).listFiles().length;
	     
	     if( !(countofFiles == 0)){
		Dataset<Row> UpdatedNpis = spark.read().option("header","true")
				.json(jsonpathUpdatedNPI+"/*.json");
	 
		 MongoSpark.save(UpdatedNpis.write().option("collection",notificationCollection).mode("overwrite"));
		 
		 long f = collectionNotify.count();
		 System.out.println("updated npis count"+f);
		 notification2trackhis();
	     }
		 Date enddate = new Date();
		
		 
		 //call multithread to doenloasd all attr 
		 
		 
		 
		
		 Downloadingzip();
		 
		 	
	
		 
	}



	public static JSONObject readJsonFromUrl(String url) throws IOException, JSONException {
		InputStream is = new URL(url).openStream();
		try {
			BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
			String jsonText = readAll(rd);
			JSONObject json = new JSONObject(jsonText);
			return json;
		} finally {
			is.close();
		}
	}
	private static String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}

	public static ArrayList<String> SplitParam(String split) {
		ArrayList<String> webParameters = new ArrayList<String>();
		for (String s : split.split(",")) {
			webParameters.add(s);

		}
		return webParameters;

	}
	public  static class MyCallable implements Callable<String> {
		String uri;
		 
		String lastupdate;

		MyCallable(String uri, String lastupdate)
		{
			this.uri=uri;
		 
			this.lastupdate=lastupdate;

		}


		public String call() throws Exception { 
			JSONObject json1;
			JSONObject json2 = new JSONObject(); 

			json1 = readJsonFromUrl("https://npiregistry.cms.hhs.gov/api/?number="+uri);
			Map<String, Object> flattenJsonMap = JsonFlattener.flattenAsMap(json1.toString()); 
			
			String[] lud = lastupdate.split("/");
			String lastdate = lud[2]+"-"+lud[0]+"-"+lud[1];
			

		 
			String path ="/home/"+user+"/Documents/JsonFilesUpdatedNPI1/";
			if(!flattenJsonMap.get("results[0].basic.last_updated").equals(null) && !flattenJsonMap.get("results[0].basic.last_updated").equals(lastdate)){
				//////
				for (Map.Entry<String,Object> entry : flattenJsonMap.entrySet()) {
					String key = entry.getKey();
					//get index of web parameter and match with db parameter replace with equavlient name 
					int y = webParam.indexOf(key);
					// int x = dbParam.indexOf(key);
					//check whether it is there are not 
					if( y >-1){
						///key is there in dB
						//System.out.println(key +"   "+dbParam.get(y) + "  "+ entry.getValue());
						//if partial download only npi and lud
					 
							Object value =  entry.getValue();
							String head= dbParam.get(y).toString().trim().replaceAll(" ","_").replaceAll("[()]","");
							json2.put(head, value.toString());
						 
					}  
				}
				String jsonStr = JsonFlattener.flatten(json2.toString());
				/////
			try (FileWriter file = new FileWriter(path+uri+".json",false)) {
				//System.out.println("over witing file");
				//file.write((""));
				file.write(jsonStr);
				file.flush();
				file.close();

			} catch (IOException e) {
				e.printStackTrace();
			} 
			}
			return ""; 
		}

	}

	public static ArrayList<String> Splitting(String split) {
		ArrayList<String> webParameters = new ArrayList<String>();
		for (String s : split.split(",")) {
			webParameters.add(s.trim());

		}
		return webParameters;

	}
	////Notification to Trackhistory and Notification to CMSData (whole collection)
	public static void notification2trackhis() {
		
		  Document notifyDoc,orginalDoc;
		  Map<String,String> trackhislist = new HashMap<String, String>();
		  Set<String> notifykeys;
		  BasicDBObject NPIquery;
		
		// TODO Auto-generated method stub
	//	long startTime = System.currentTimeMillis();
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		
		 
		MongoCollection<Document> wholecoll = collection;
		MongoCollection<Document> notifycoll =collectionNotify;
		MongoCollection<Document> TrackHistory = collectionTrackNpi;
		//notification collection iterating
		FindIterable<Document> notifysearch = notifycoll.find().noCursorTimeout(true);;
		Iterator<Document> notifyIter = notifysearch.iterator();
		//notification collection while loop
		while(notifyIter.hasNext()){
			
			
			notifyDoc = notifyIter.next();
		 notifykeys = notifyDoc.keySet();
			NPIquery = new BasicDBObject("NPI",notifyDoc.get("NPI"));
			
			//wholecollection iterating
			FindIterable<Document> originalsearch = wholecoll.find(NPIquery).limit(1).noCursorTimeout(true);
			Iterator<Document> orgIter = originalsearch.iterator();
			//whole collection while loop
			if(orgIter.hasNext()){
				
				orginalDoc = orgIter.next();
				
				for(String keys: notifykeys){
			
					
					if(!keys.equalsIgnoreCase("_id") && !keys.equalsIgnoreCase("NPI")){
						
						String orgcolldata = orginalDoc.getString(keys);
						String notiycolldata = notifyDoc.getString(keys);
						
						if(!notiycolldata.equalsIgnoreCase(orgcolldata)){
							trackhislist.put(keys, notiycolldata);
							trackhislist.put("Past_"+keys, orgcolldata);
							
							  if(keys.matches(".*Date.*"))
				                {
				                String [] sp= notiycolldata.split("-");
				                notiycolldata = sp[1]+"/"+sp[2]+"/"+sp[0];
				                 
				                }
							
							BasicDBObject org = new BasicDBObject("$set", new BasicDBObject(keys,notiycolldata));
							wholecoll.updateOne(NPIquery, org);
							
			
							
						}
						
					}
				}
				//TrachHistory search
				FindIterable<Document> trackhissearch = TrackHistory.find(NPIquery).limit(1).noCursorTimeout(true);
				Iterator<Document> trackhisIter = trackhissearch.iterator();
				 Boolean bool = trackhisIter.hasNext();
				 
				 if(bool.equals(null) || bool.equals(true)){			
				
				//TrackHistory  updating
				BasicDBObject updateNotification = new BasicDBObject("$set",new BasicDBObject(dateFormat.format(date), trackhislist));
				TrackHistory.updateOne(NPIquery, updateNotification);
				 
				 }else{
					 
						Document trackdoc = new Document("NPI", notifyDoc.get("NPI"))
								.append(dateFormat.format(date), trackhislist);
						TrackHistory.insertOne(trackdoc);
					 
				 }
				 
				
			}
			
			trackhislist.clear();
			
		}


	}
	

	

	
	//Downloading the zip file from the NPI site
	 public static void Downloadingzip() throws IOException, JSONException, InterruptedException {
	        
	        MongoClient client = new MongoClient("localhost", 27081);
	        MongoDatabase DB = client.getDatabase("Provider");
	        MongoCollection<Document> boolcoll = DB.getCollection("CMSZipBoolean");
	        
	            //String saveTo = "/home/exa1/Documents/JsonFilesP/";
	          LocalDateTime mon = LocalDateTime.now();
	          int year = mon.getYear();
	          int date = mon.getDayOfMonth();
	          String month = mon.getMonth().toString();
	          String Month="";
	          if(month.equals("JANUARY"))
	              Month = "January";
	          else if(month.equals("FEBRUARY"))
	              Month = "February";
	          else if(month.equals("MARCH"))
	              Month = "March";
	          else if(month.equals("APRIL"))
	              Month = "April";
	          else if(month.equals("MAY"))
	              Month = "May";
	          else if(month.equals("JUNE"))
	              Month = "June";
	          else if(month.equals("JULY"))
	              Month = "July";
	          else if(month.equals("AUGUST"))
	              Month = "August";
	          else if(month.equals("SEPTEMBER"))
	              Month = "September";
	          else if(month.equals("OCTOBER"))
	              Month = "October";
	          else if(month.equals("NOVEMBER"))
	              Month = "November";
	          else if(month.equals("DECEMBER"))
	              Month = "December";
	          BasicDBObject zipb = new BasicDBObject("ZipIndex","CMS");
	          FindIterable<Document> search = boolcoll.find(zipb);
	          Iterator<Document> Iter = search.iterator();
	         System.out.println(date);
	          if(date == 16){  
	              
	              if(Iter.hasNext()){              
	              Document doc = Iter.next();
	                  boolean bool = doc.getBoolean("ZipBoolean");
	             if(bool == false) {
	            
	         try {
	              URL url = new URL("http://download.cms.gov/nppes/NPPES_Data_Dissemination_"+Month+"_"+year+".zip");
	              URLConnection conn = url.openConnection();
	              InputStream in = conn.getInputStream();
	              FileOutputStream out = new FileOutputStream(zipfilepath + ".zip");
	              byte[] b = new byte[1024];
	              int count;
	              while ((count = in.read(b)) >= 0) {
	                  out.write(b, 0, count);
	              }
	              out.flush(); out.close(); in.close();                   
	   
	          } catch (IOException e) {
	              e.printStackTrace();
	                                   }
	        
	        System.out.println("going for unzip"); 
	  File file = new File(zipfilepath);
	  if(file.isDirectory()){
	      
	      File[] files = file.listFiles();

	      unZipIt(files[0].toString(), zipfilepath);
	  } 
	      //System.out.println("Format");
	      if(file.isDirectory()){
	          File[] pat = file.listFiles();
	        //  System.out.println(pat[0].toString());
	          Format(pat[0].toString());      
	      }
	      BasicDBObject update = new BasicDBObject("$set", new BasicDBObject("ZipBoolean", true));
	              boolcoll.updateOne(zipb, update);
	          }
	             }
	    }else{
	        BasicDBObject update = new BasicDBObject("$set", new BasicDBObject("ZipBoolean", false));
	          boolcoll.updateOne(zipb, update);
	          
	        //  System.out.println("Today");
	          
	          }}

	    public static void unZipIt(String zipFile, String outputFolder){

	        byte[] buffer = new byte[1024];
	           
	        try{

	           ZipInputStream zis = 
	               new ZipInputStream(new FileInputStream(zipFile));
	           //get the zipped file list entry
	           ZipEntry ze = zis.getNextEntry();
	               
	           while(ze!=null){
	                   
	              String fileName = ze.getName();
	              File newFile = new File(outputFolder + File.separator + fileName);
	                   
	             // System.out.println("file unzip : "+ newFile.getAbsoluteFile());
	                   
	               //create all non exists folders
	               //else you will hit FileNotFoundException for compressed folder
	               new File(newFile.getParent()).mkdirs();
	                 
	               FileOutputStream fos = new FileOutputStream(newFile);             

	               int len;
	               while ((len = zis.read(buffer)) > 0) {
	                  fos.write(buffer, 0, len);
	               }
	                   
	               fos.close();   
	               ze = zis.getNextEntry();
	           }
	           
	           zis.closeEntry();
	           zis.close();
	         File str = new File(zipfilepath);
	      File[] files = str.listFiles();
	      for(File f : files){
	          
	          String s = f.getAbsolutePath();
	          if(s.matches(".*.zip"))
	              f.delete();
	      } 
	               
	          // System.out.println("Done");
	               
	       }catch(IOException ex){
	          ex.printStackTrace(); 
	       }
	      }    
	    
	    
	    public static void Format(String pathofcsv) throws IOException, JSONException, InterruptedException {
	        
	        //MongoCollection<Document> dropcms = database.getCollection("CMSData");
	       // dropcms.drop();
	        String path = pathofcsv.trim();
	        SparkConf configuration = new SparkConf().setAppName("inserting data").setMaster("local[*]");
	        SparkSession sparks = SparkSession
	                .builder()
	                .config(configuration)
	                .config("spark.mongodb.input.uri", "mongodb://localhost:27081/Provider.CMSData?compressors=zlib")
	                .config("spark.mongodb.output.uri", "mongodb://localhost:27081/Provider")
	                .config("spark.executor.cores","4")
	                .config("spark.executor.memory", "5G")
	                .getOrCreate();
	        //logicCode(spark);

	 Dataset<Row> DB01 = sparks.read().option("header","true").csv(path);
	 Dataset<Row> DB0 = renameTable("",DB01);
	 MongoSpark.save(DB0.na().fill("").write().option("collection", "CMSData").mode("overwrite"));
	 
	 //deleting all the folders in the path
	 String jsonpathUpdatedNPI = zipfilepath;
	 Arrays.stream(new File(jsonpathUpdatedNPI).listFiles()).forEach(File::delete);
	        
	        
	    }
	    private static Dataset<Row> renameTable(String table, Dataset<Row> hcd) {

	        Dataset<Row> hcdf=hcd;
	        String col[]=hcd.schema().fieldNames();
	        for(String tablecol:col){
	            //remove all regex and spaces 
	            String h= tablecol.replaceAll(" ","_").replaceAll("[(.)]","");
	            ///    System.out.println(tablecol +"-----"+h);
	            hcdf=  hcdf.withColumnRenamed((String)tablecol,h);  

	            String [] hh = hcdf.columns();

	        }
	        return hcdf;
	    }
	
}