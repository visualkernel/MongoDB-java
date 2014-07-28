package com.ice.db.mongodb;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

public class MongoDBHelper {

	private Logger logger = Logger.getLogger(MongoDBHelper.class);

	private DB db = null;
	private DBCollection collection = null;
	private GridFS fs = null;

	public MongoDBHelper() {
		db = MongoDBClient.getDB();
		fs = new GridFS(db);
		logger.info("New [MongoDBHelper], database:[" + db + "]");
	}

	public DB getDB() {
		return db;
	}

	public MongoDBHelper(String collectionName) {
		db = MongoDBClient.getDB();
		collection = db.getCollection(collectionName);
		logger.info("New [MongoDBHelper], database:[" + db
				+ "], collection: [" + collectionName + "]");
	}

	public DBCollection getCollection() {
		return collection;
	}

	public void setCollection(String collectionName) {
		collection = db.getCollection(collectionName);
	}

	public DBObject findOne() {
		logger.info("findOne => collection: " + collection.getName());
		
		return collection.findOne();
	}

	public DBObject findOne(DBObject ref) {
		logger.info("findOne => collection: " + collection.getName()
				+ ", ref: " + ref);
		
		return collection.findOne(ref);
	}
	
	public DBObject findOne(DBObject ref, DBObject keys) {
		logger.info("findOne => collection: " + collection.getName()
				+ ", ref: " + ref + ", keys: " + keys);
		
		return collection.findOne(ref, keys);
	}

	public DBCursor find() {
		logger.info("find => collection: " + collection.getName());
		
		return collection.find();
	}

	public DBCursor find(DBObject ref) {
		logger.info("find => collection: " + collection.getName() + ", ref: "
				+ ref);

		return collection.find(ref);
	}

	public DBCursor find(DBObject ref, DBObject keys) {
		logger.info("find => collection: " + collection.getName() + ", ref: "
				+ ref + ", keys: " + keys);

		return collection.find(ref, keys);
	}

	public DBCursor find(int limit) {
		logger.info("find => collection: " + collection.getName() + ", limit: "
				+ limit);

		return collection.find().limit(limit);
	}

	public DBCursor find(DBObject ref, int limit) {
		logger.info("find => collection: " + collection.getName() + ", ref: "
				+ ref + ", limit: " + limit);

		return collection.find(ref).limit(limit);
	}
	
	public DBCursor find(DBObject ref, DBObject keys, int limit) {
		logger.info("find => collection: " + collection.getName() + ", ref: "
				+ ref + ", keys:" + keys + ", limit: " + limit);

		return collection.find(ref, keys).limit(limit);
	}


	public DBCursor find(int skip, int limit) {
		logger.info("find => collection: " + collection.getName() + ", limit: "
				+ limit);

		return collection.find().skip(skip).limit(limit);
	}

	public DBCursor find(DBObject ref, int skip, int limit) {
		logger.info("find => collection: " + collection.getName() + ", ref: "
				+ ref + ", limit: " + limit);

		return collection.find(ref).skip(skip).limit(limit);
	}
	
	public DBCursor find(DBObject ref, DBObject keys, int skip, int limit) {
		logger.info("find => collection: " + collection.getName() + ", ref: "
				+ ref + ", keys: " + keys + ",limit: " + limit);

		return collection.find(ref, keys).skip(skip).limit(limit);
	}

	public <T> T findOne(Class<T> toObjectClass,
			Map<String, String> replaceKeyMap) {
		logger.info("findOne => collection: " + collection.getName()
				+ ", toObjectClass: " + toObjectClass + ", replaceKeyMap:"
				+ replaceKeyMap);

		DBObject obj = findOne();
		T rtobj = MongoDBUtil
				.convertDBObject(obj, toObjectClass, replaceKeyMap);

		return rtobj;
	}

	public <T> T findOne(DBObject ref, Class<T> toObjectClass,
			Map<String, String> replaceKeyMap) {
		logger.info("findOne => collection: " + collection.getName()
				+ ", ref: " + ref + ", toObjectClass: " + toObjectClass
				+ ", replaceKeyMap:" + replaceKeyMap);

		T rtobj = null;
		DBObject obj = findOne(ref);
		rtobj = MongoDBUtil.convertDBObject(obj, toObjectClass, replaceKeyMap);

		return rtobj;
	}
	
	public <T> T findOne(DBObject ref, DBObject keys, Class<T> toObjectClass,
			Map<String, String> replaceKeyMap) {
		logger.info("findOne => collection: " + collection.getName()
				+ ", ref: " + ref + ", keys: " + keys + ", toObjectClass: " + toObjectClass
				+ ", replaceKeyMap:" + replaceKeyMap);

		T rtobj = null;
		DBObject obj = findOne(ref, keys);
		rtobj = MongoDBUtil.convertDBObject(obj, toObjectClass, replaceKeyMap);

		return rtobj;
	}

	public <T> List<T> find(Class<T> toObjectClass,
			Map<String, String> replaceKeyMap) {
		logger.info("find => collection: " + collection.getName()
				+ ", toObjectClass: " + toObjectClass + ", replaceKeyMap:"
				+ replaceKeyMap);

		List<T> list = new ArrayList<T>();
		DBCursor cur = find();
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}

	public <T> List<T> find(DBObject ref, Class<T> toObjectClass,
			Map<String, String> replaceKeyMap) {
		logger.info("find => collection: " + collection.getName() + ", ref: "
				+ ref + ", toObjectClass: " + toObjectClass
				+ ", replaceKeyMap:" + replaceKeyMap);

		List<T> list = new ArrayList<T>();
		DBCursor cur = find(ref);
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}
	
	public <T> List<T> find(DBObject ref, DBObject keys, Class<T> toObjectClass,
			Map<String, String> replaceKeyMap) {
		logger.info("find => collection: " + collection.getName() + ", ref: "
				+ ref + ", keys: " + keys + ", toObjectClass: " + toObjectClass
				+ ", replaceKeyMap:" + replaceKeyMap);

		List<T> list = new ArrayList<T>();
		DBCursor cur = find(ref, keys);
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}

	public <T> List<T> find(int limit, Class<T> toObjectClass,
			Map<String, String> replaceKeyMap) {
		logger.info("find => collection: " + collection.getName()
				+ ", limit: " + limit + ", toObjectClass: " + toObjectClass
				+ ", replaceKeyMap:" + replaceKeyMap);

		List<T> list = new ArrayList<T>();
		DBCursor cur = find(limit);
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}

	public <T> List<T> find(DBObject ref, int limit, Class<T> toObjectClass,
			Map<String, String> replaceKeyMap) {
		logger.info("find => collection: " + collection.getName() + ", ref: "
				+ ref + ", limit: " + limit + ", toObjectClass: "
				+ toObjectClass + ", replaceKeyMap:" + replaceKeyMap);

		List<T> list = new ArrayList<T>();
		DBCursor cur = find(ref, limit);
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}
	
	public <T> List<T> find(DBObject ref, DBObject keys, int limit, Class<T> toObjectClass,
			Map<String, String> replaceKeyMap) {
		logger.info("find => collection: " + collection.getName() + ", ref: "
				+ ref + ", keys: " + keys + ", limit: " + limit + ", toObjectClass: "
				+ toObjectClass + ", replaceKeyMap:" + replaceKeyMap);

		List<T> list = new ArrayList<T>();
		DBCursor cur = find(ref, keys, limit);
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}

	public <T> List<T> find(int skip, int limit, Class<T> toObjectClass,
			Map<String, String> replaceKeyMap) {
		logger.info("find => collection: " + collection.getName() + ", skip: "
				+ skip + ", limit: " + limit + ", toObjectClass: "
				+ toObjectClass + ", replaceKeyMap:" + replaceKeyMap);

		List<T> list = new ArrayList<T>();
		DBCursor cur = find(skip, limit);
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}

	public <T> List<T> find(DBObject ref, int skip, int limit,
			Class<T> toObjectClass, Map<String, String> replaceKeyMap) {
		logger.info("find => collection: " + collection.getName() + ", ref: "
				+ ref + ", skip: " + skip + ", limit: " + limit
				+ ", toObjectClass: " + toObjectClass + ", replaceKeyMap:"
				+ replaceKeyMap);

		List<T> list = new ArrayList<T>();
		DBCursor cur = find(ref, skip, limit);
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}
	
	public <T> List<T> find(DBObject ref, DBObject keys, int skip, int limit,
			Class<T> toObjectClass, Map<String, String> replaceKeyMap) {
		logger.info("find => collection: " + collection.getName() + ", ref: "
				+ ref + ", keys: " + keys + ", skip: " + skip + ", limit: " + limit
				+ ", toObjectClass: " + toObjectClass + ", replaceKeyMap:"
				+ replaceKeyMap);

		List<T> list = new ArrayList<T>();
		DBCursor cur = find(ref, keys, skip, limit);
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}

	public <T> List<T> findAndSort(DBObject sort, Class<T> toObjectClass,
			Map<String, String> replaceKeyMap) {
		logger.info("findAndSort => collection: " + collection.getName()
				+ ", sort: " + sort + ", toObjectClass: " + toObjectClass
				+ ", replaceKeyMap:" + replaceKeyMap);

		List<T> list = new ArrayList<T>();
		DBCursor cur = find().sort(sort);
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}

	public <T> List<T> findAndSort(DBObject ref, DBObject sort,
			Class<T> toObjectClass, Map<String, String> replaceKeyMap) {
		logger.info("findAndSort => collection: " + collection.getName()
				+ ", ref: " + ref + ", sort: " + sort + ", toObjectClass: "
				+ toObjectClass + ", replaceKeyMap:" + replaceKeyMap);

		List<T> list = new ArrayList<T>();
		DBCursor cur = find(ref).sort(sort);
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}
	
	public <T> List<T> findAndSort(DBObject ref, DBObject keys, DBObject sort,
			Class<T> toObjectClass, Map<String, String> replaceKeyMap) {
		logger.info("findAndSort => collection: " + collection.getName()
				+ ", ref: " + ref + ", keys: " + keys + ", sort: " + sort + ", toObjectClass: "
				+ toObjectClass + ", replaceKeyMap:" + replaceKeyMap);

		List<T> list = new ArrayList<T>();
		DBCursor cur = find(ref, keys).sort(sort);
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}

	public <T> List<T> findAndSort(DBObject sort, int limit,
			Class<T> toObjectClass, Map<String, String> replaceKeyMap) {
		logger.info("findAndSort => collection: " + collection.getName()
				+ ", sort: " + sort + ", limit: " + limit + ", toObjectClass: "
				+ toObjectClass + ", replaceKeyMap:" + replaceKeyMap);
		List<T> list = new ArrayList<T>();
		DBCursor cur = find(limit).sort(sort);
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}

	public <T> List<T> findAndSort(DBObject sort, int skip, int limit,
			Class<T> toObjectClass, Map<String, String> replaceKeyMap) {
		logger.info("findAndSort => collection: " + collection.getName()
				+ ", sort: " + sort + ", limit: " + limit + ", toObjectClass: "
				+ toObjectClass + ", replaceKeyMap:" + replaceKeyMap);

		List<T> list = new ArrayList<T>();
		DBCursor cur = find(skip, limit).sort(sort);
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}

	public <T> List<T> findAndSort(DBObject ref, DBObject sort, int limit,
			Class<T> toObjectClass, Map<String, String> replaceKeyMap) {
		logger.info("findAndSort => collection: " + collection.getName()
				+ ", ref: " + ref + ", sort: " + sort + ", limit: " + limit
				+ ", toObjectClass: " + toObjectClass + ", replaceKeyMap:"
				+ replaceKeyMap);

		List<T> list = new ArrayList<T>();
		DBCursor cur = find(ref, limit).sort(sort);
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}
	
	public <T> List<T> findAndSort(DBObject ref, DBObject keys, DBObject sort, int limit,
			Class<T> toObjectClass, Map<String, String> replaceKeyMap) {
		logger.info("findAndSort => collection: " + collection.getName()
				+ ", ref: " + ref + ", keys: " + keys + ", sort: " + sort + ", limit: " + limit
				+ ", toObjectClass: " + toObjectClass + ", replaceKeyMap:"
				+ replaceKeyMap);

		List<T> list = new ArrayList<T>();
		DBCursor cur = find(ref, keys, limit).sort(sort);
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}

	public <T> List<T> findAndSort(DBObject ref, DBObject sort, int skip,
			int limit, Class<T> toObjectClass, Map<String, String> replaceKeyMap) {
		logger.info("findAndSort => collection: " + collection.getName()
				+ ", ref: " + ref + ", sort: " + sort + ", limit: " + limit
				+ ", toObjectClass: " + toObjectClass + ", replaceKeyMap:"
				+ replaceKeyMap);

		List<T> list = new ArrayList<T>();
		DBCursor cur = find(ref, skip, limit).sort(sort);
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}
	
	public <T> List<T> findAndSort(DBObject ref, DBObject keys, DBObject sort, int skip,
			int limit, Class<T> toObjectClass, Map<String, String> replaceKeyMap) {
		logger.info("findAndSort => collection: " + collection.getName()
				+ ", ref: " + ref + ", keys: " + keys + ", sort: " + sort + ", limit: " + limit
				+ ", toObjectClass: " + toObjectClass + ", replaceKeyMap:"
				+ replaceKeyMap);

		List<T> list = new ArrayList<T>();
		DBCursor cur = find(ref, keys, skip, limit).sort(sort);
		list = MongoDBUtil.convertDBCursor(cur, toObjectClass, replaceKeyMap);

		return list;
	}

	public WriteResult insert(DBObject... arr) {
		logger.info("insert => collection: " + collection.getName()
				+ ", arr: " + arr);

		return collection.insert(arr);
	}

	public WriteResult insert(Object fromObject,
			Map<String, String> replaceKeyMap) {
		DBObject obj = MongoDBUtil.convertDBObject(fromObject, replaceKeyMap);
		return insert(obj);
	}

	public WriteResult insert(List<DBObject> list) {
		logger.info("insert => collection: " + collection.getName()
				+ ", list: " + list);

		return collection.insert(list);
	}

	public WriteResult insert(List<Object> fromList,
			Map<String, String> replaceKeyMap) {
		List<DBObject> objList = MongoDBUtil.convertDBObjectList(fromList,
				replaceKeyMap);

		return insert(objList);
	}

	public WriteResult update(DBObject q, DBObject o) {
		logger.info("update => collection: " + collection.getName() + ", q: "
				+ q + ", o:" + o);

		return collection.update(q, o);
	}

	public WriteResult update(DBObject q, Object fromObject,
			Map<String, String> replaceKeyMap) {
		DBObject o = MongoDBUtil.convertDBObject(fromObject, replaceKeyMap);

		return update(q, o);
	}

	public WriteResult updateMulti(DBObject q, DBObject o) {
		logger.info("updateMulti => collection: " + collection.getName()
				+ ", q: " + q + ", o:" + o);
		return collection.updateMulti(q, o);
	}

	public WriteResult updateMulti(DBObject q, Object fromObject,
			Map<String, String> replaceKeyMap) {
		DBObject o = MongoDBUtil.convertDBObject(fromObject, replaceKeyMap);
		return updateMulti(q, o);
	}

	public WriteResult update(DBObject q, DBObject o, boolean upsert,
			boolean multi) {
		logger.info("update => collection: " + collection.getName() + ", q: "
				+ q + ", o:" + o + ", upsert: " + upsert + ", upsert: "
				+ upsert + ", multi: " + multi);
		return collection.update(q, o, upsert, multi);
	}

	public WriteResult update(DBObject q, Object fromObject,
			Map<String, String> replaceKeyMap, boolean upsert, boolean multi) {
		DBObject o = MongoDBUtil.convertDBObject(fromObject, replaceKeyMap);
		return update(q, o, upsert, multi);
	}

	public WriteResult delete(DBObject ref) {
		logger.info("delete => collection: " + collection.getName()
				+ ", ref: " + ref);
		return collection.remove(ref);
	}

	public WriteResult delete(Object fromObject,
			Map<String, String> replaceKeyMap) {
		DBObject ref = MongoDBUtil.convertDBObject(fromObject, replaceKeyMap);
		return delete(ref);
	}

	public WriteResult deleteByObjectId(String _id) {
		ObjectId id = new ObjectId(_id);
		return deleteByObjectId(id);
	}

	public WriteResult deleteByObjectId(ObjectId _id) {
		logger.info("deleteByObjectId => collection: " + collection.getName()
				+ ", _id: " + _id);
		DBObject ref = new BasicDBObject("_id", _id);
		return collection.remove(ref);
	}

	public CommandResult getLastError() {
		return db.getLastError();
	}
	
	public void saveFile(DBObject ref, File file, String filename, String contentType) throws IOException {
		GridFS fs = new GridFS(db);
		GridFSInputFile inputFile = fs.createFile(file);
		inputFile.setFilename(filename);
		inputFile.setContentType(contentType);
		
		if (ref != null) {
			fs.remove(ref);
		}
		logger.info("saveFile => ref:" + ref + ", fs:" + fs);
		inputFile.save();
	}
	
	public void saveFile(DBObject ref, InputStream in, String filename, String contentType) throws IOException {
		GridFS fs = new GridFS(db);
		GridFSInputFile inputFile = fs.createFile(in);
		inputFile.setFilename(filename);
		inputFile.setContentType(contentType);
		
		if (ref != null) {
			fs.remove(ref);
		}
		logger.info("saveFile => ref:" + ref + ", fs:" + fs);
		inputFile.save();
	}
	
	public void saveFile(DBObject ref, File file, Map<String, Object> map) throws IOException {
		GridFSInputFile inputFile = fs.createFile(file);
		
		if (map != null) {
			Object value = null;
			Set<String> keySet = map.keySet();
			for (String key : keySet) {
				value = map.get(key);
				inputFile.put(key, value);
			}
			
		}
		
		if (ref != null) {
			fs.remove(ref);
		}
		logger.info("saveFile => ref:" + ref + ", fs:" + fs);
		inputFile.save();
	}
	
	public void saveFile(DBObject ref, InputStream in, Map<String, Object> map) throws IOException {
		GridFSInputFile inputFile = fs.createFile(in);
		
		if (map != null) {
			Object value = null;
			Set<String> keySet = map.keySet();
			for (String key : keySet) {
				value = map.get(key);
				inputFile.put(key, value);
			}
			
		}
		
		if (ref != null) {
			fs.remove(ref);
		}
		logger.info("saveFile => ref:" + ref + ", fs:" + fs);
		inputFile.save();
	}
	
	public void removeFile(DBObject ref) {
		logger.info("removeFile => ref:" + ref);
		fs.remove(ref);
	}
	
	public GridFSDBFile findFile(DBObject ref) {
		logger.info("findFile => ref:" + ref);
		return fs.findOne(ref);
	}
	
	public GridFSDBFile findFile(ObjectId id) {
		logger.info("findFile => id:" + id);
		return fs.findOne(id);
	}
	
	public GridFSDBFile findFile(String filename) {
		logger.info("findFile => filename:" + filename);
		return fs.findOne(filename);
	}
	
	public List<GridFSDBFile> findFiles(DBObject ref) {
		logger.info("findFile => ref:" + ref);
		return fs.find(ref);
	}
}
