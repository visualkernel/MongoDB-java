package com.ice.db.mongodb;

import java.util.ResourceBundle;

import org.apache.log4j.Logger;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;

public class MongoDBClient {

	private MongoDBClient() {
	}

	private static MongoClient client = null;
	private static String dbname = null;
	private static Logger logger = Logger.getLogger(MongoDBClient.class);

	static {
		if (client == null) {
			synchronized (MongoDBClient.class) {
				try {
					// loads mongodb configure file mongodb.properties
					ResourceBundle rb = ResourceBundle.getBundle("mongodb");
					

					//reads uri from db.properties and creates a client connection to server
					String uri = rb.getString("db.uri");
					logger.info("db.uri : " + uri);
					if (uri == null || uri.trim().equals("")) {
						// default localhost
						client = new MongoClient();
					} else {
						MongoClientURI clientURI = new MongoClientURI(uri);
						dbname = clientURI.getDatabase();
						client = new MongoClient(clientURI);
						client.setReadPreference(ReadPreference.secondaryPreferred());
					}
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
					e.printStackTrace();
				}
			}
		}
	}

	public static MongoClient getMongoClient() {
		return client;
	}

	public static DB getDB() {
		logger.info("connection to " + dbname);
		return client.getDB(dbname);
	}

	public static void close() {
		client.close();
	}
}