package com.ice.db.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoDBUtil {

	private Logger logger = Logger.getLogger(MongoDBUtil.class);

	/**
	 * 将fromObject对象转换成toObjectClass类型的对象
	 * 
	 * @param fromObject
	 *            源对象
	 * @param toObjectClass
	 *            目标对象的类型
	 * @return T 目标对象
	 * 
	 */
	public static <T> T convert(Object fromObject, Class<T> toObjectClass) {

		ObjectMapper objectMapper = new ObjectMapper();
		// String json = objectMapper.writeValueAsString(fromObject);
		// obj = objectMapper.readValue(json, toObjectClass);

		return objectMapper.convertValue(fromObject, toObjectClass);
	}

	/**
	 * 将对象fromObject转换成DBObject对象
	 * 
	 * @param fromObject
	 * @param replaceKeyMap
	 *            根据replaceKeyMap用key的值替换DBObject中key名，如果key的值为null，则去掉相应的key
	 * @return DBObject
	 * 
	 */
	public static DBObject convertDBObject(Object fromObject,
			Map<String, String> replaceKeyMap) {
		if (fromObject == null) {
			return null;
		}

		DBObject dbObject = null;

		dbObject = convert(fromObject, BasicDBObject.class);
		if (replaceKeyMap != null && replaceKeyMap.size() > 0) {
			Set<String> set = replaceKeyMap.keySet();
			String newKey = null;
			for (String oldKey : set) {
				newKey = replaceKeyMap.get(oldKey);
				if (newKey != null) {
					Object val = dbObject.get(oldKey);
					dbObject.put(newKey, val);
				}
				dbObject.removeField(oldKey);
			}
		}
		return dbObject;
	}

	/**
	 * 将对象DBObject转换成toObjectClass类型对象
	 * 
	 * @param dbObject
	 * @param toObjectClass
	 * @param replaceKeyMap
	 *            根据replaceKeyMap用key的值替换DBObject中key名，如果key的值为null，则去掉相应的key
	 * @return T
	 * 
	 */
	public static <T> T convertDBObject(DBObject dbObject,
			Class<T> toObjectClass, Map<String, String> replaceKeyMap) {
		if (dbObject == null) {
			return null;
		}
		// hack for mongodb [_id] field
		if (dbObject.containsField("_id")) {
			Object val = null;
			String str = null;
			val = dbObject.get("_id");
			if (val.getClass() != String.class && val != null) {
				str = val.toString();
				dbObject.put("_id", str);
			}
		}
		// handle key map
		if (replaceKeyMap != null && replaceKeyMap.size() > 0) {
			Set<String> set = replaceKeyMap.keySet();
			String newKey = null;
			for (String oldKey : set) {
				newKey = replaceKeyMap.get(oldKey);
				if (newKey != null) {
					Object val = dbObject.get(oldKey);
					dbObject.put(newKey, val);
				}
				dbObject.removeField(oldKey);
			}
		}
		return convert(dbObject, toObjectClass);
	}

	/**
	 * 将游标DBCursor中的记录转成包含类toObjectClass对象的集合
	 * 
	 * @param cursor
	 * @param toObjectClass
	 * @param replaceKeyMap
	 * @return List<T>
	 * 
	 */
	public static <T> List<T> convertDBCursor(DBCursor cursor,
			Class<T> toObjectClass, Map<String, String> replaceKeyMap) {
		List<T> list = new ArrayList<T>();

		if (cursor == null) {
			return list;
		}

		while (cursor.hasNext()) {
			list.add((T) MongoDBUtil.convertDBObject(cursor.next(),
					toObjectClass, replaceKeyMap));
		}

		return list;
	}

	/**
	 * 将一组Pojo对象转换成一组DBObject对象
	 * 
	 * @param fromList
	 *            pojo对象列表
	 * @param replaceKeyMap
	 *            根据replaceKeyMap用key的值替换DBObject中key名，如果key的值为null，则去掉相应的key
	 * @return
	 */
	public static List<DBObject> convertDBObjectList(List<Object> fromList,
			Map<String, String> replaceKeyMap) {
		if (fromList == null) {
			return null;
		}

		List<DBObject> dbobjList = new ArrayList<DBObject>();

		for (Object fromObject : fromList) {
			DBObject dbObject = convertDBObject(fromObject, replaceKeyMap);
			dbobjList.add(dbObject);
		}

		return dbobjList;
	}

}
