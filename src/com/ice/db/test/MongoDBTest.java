/**
 * @file MongoDBTest.java
 * @author Administrator
 * @create 2014年7月28日
 */
package com.ice.db.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ice.db.mongodb.MongoDBHelper;

/**
 * @author Administrator
 * @create 2014年7月28日
 * @version 0.1
 *
 */
public class MongoDBTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		MongoDBHelper helper = new MongoDBHelper("book");
		Map<String, String> replaceKeyMap = new HashMap<String, String>();
		replaceKeyMap.put("_id", "id");
		
		List<Book> bookList = helper.find(Book.class, replaceKeyMap);
		
		for (Book book : bookList) {
			System.out.println(book.getId() + ":" + book.getName());
		}
	}
}