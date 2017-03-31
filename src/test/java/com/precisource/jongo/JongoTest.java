package com.precisource.jongo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.*;
import com.precisource.jongo.model.Drama;
import org.bson.types.ObjectId;
import org.jongo.*;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * @author zanxus
 * @version 1.0.0
 * @date 2017-03-29 下午6:05
 * reference link: http://jongo.org/
 */
public class JongoTest {


    private static MongoCollection collection;

    private static MongoCursor cursor;

    private static final String HOST = "localhost";
    private static final Integer PORT = 27017;
    private static final String DB_NAME = "video";
    private static final String COLLECTION_NAME = "USDramas";

    private static ObjectMapper objectMapper = new ObjectMapper();

    static {
        DB db = new MongoClient(HOST, PORT).getDB(DB_NAME);
        Jongo jongo = new Jongo(db);
        collection = jongo.getCollection(COLLECTION_NAME);
    }

    private static void writeJSON(Object o) throws JsonProcessingException {
        System.out.println(objectMapper.writeValueAsString(o));
    }

    private static void iterate(Iterator iterator) throws JsonProcessingException {
        while (iterator.hasNext()) {
            writeJSON(iterator.next());
        }
    }

//    @Test
//    public void writeData2File() throws IOException {
//        cursor = collection.find().as(Drama.class);
//        Iterator<Drama> iterator = cursor.iterator();
//        List<Drama> dramas = new ArrayList<>();
//        while (iterator.hasNext()) {
//            Drama drama = iterator.next();
//            dramas.add(drama);
//        }
//        File file = new File("src/test/resources/data.json");
//        objectMapper.writeValue(file, dramas);
//    }

    @BeforeClass
    public static void setUp() throws IOException {
        cursor = collection.find().as(Drama.class);
        if (cursor.count() == 0) {
            List<Drama> dramas = objectMapper.readValue(new File("src/test/resources/data.json"), new TypeReference<List<Drama>>() {
            });
            dramas.forEach(drama -> collection.save(drama));
        }
    }


    /**
     * 根据查询条件返回集合
     *
     * @throws JsonProcessingException
     */
    @Test
    public void find() throws JsonProcessingException {

        //查询collection下所有document
        cursor = collection.find().as(Drama.class);

        // 计算查询结果数量
        int count = cursor.count();
        System.out.println(count);

        //传参查询document,值需要用单引号引起来，如果key(字段名字)包含小数点，也需要用单引号引起来
        cursor = collection.find("{day: 'monday'}").as(Drama.class);

        //find方法返回的是游标，使用迭代器遍历所有document
        iterate(cursor.iterator());

    }

    /**
     * 根据查询条件返回单个对象
     *
     * @throws JsonProcessingException
     */
    @Test
    public void findOne() throws JsonProcessingException {
        //根据指定的objectid获取对象,实际id值请替换成数据库中存在的ObjectId值
        Drama drama = collection.findOne(Oid.withOid("58dcc5214db0643b54152eaa")).as(Drama.class);
        //另一种根据objectid获取对象的方式
        Drama drama1 = collection.findOne("{ _id: #}", new ObjectId("58dcc5214db0643b54152eaa")).as(Drama.class);
        //缩写方式
        Drama drama2 = collection.findOne(new ObjectId("58dcc5214db0643b54152eaa")).as(Drama.class);
        writeJSON(drama);   //{"id":"58d2300f4db0640c1a24f007","href":"http://bbs.ncar.cc/thread-28877-1-1.html","title":"行尸走肉 第七季 The Walking Dead Season 7 (2016)[已更新14集][每周一更新][中英]","text":"行尸走肉 第七季 The Walking Dead Season ","day":"monday"}
        writeJSON(drama1); //{"id":"58d2300f4db0640c1a24f007","href":"http://bbs.ncar.cc/thread-28877-1-1.html","title":"行尸走肉 第七季 The Walking Dead Season 7 (2016)[已更新14集][每周一更新][中英]","text":"行尸走肉 第七季 The Walking Dead Season ","day":"monday"}
        writeJSON(drama2); //{"id":"58d2300f4db0640c1a24f007","href":"http://bbs.ncar.cc/thread-28877-1-1.html","title":"行尸走肉 第七季 The Walking Dead Season 7 (2016)[已更新14集][每周一更新][中英]","text":"行尸走肉 第七季 The Walking Dead Season ","day":"monday"}

    }


    /**
     * 投影方式查询
     * {field: 1} will include the field, {field: 0} will exclude the field. 未指明的字段默认过滤掉
     *
     * @throws JsonProcessingException
     */
    @Test
    public void projection() throws JsonProcessingException {
        cursor = collection.find().projection("{href:1,title:1}").as(Drama.class);
        iterate(cursor.iterator());// example one:text and day  will be null here {"id":"58d2300f4db0640c1a24f007","href":"http://bbs.ncar.cc/thread-28877-1-1.html","title":"行尸走肉 第七季 The Walking Dead Season 7 (2016)[已更新14集][每周一更新][中英]","text":null,"day":null}
    }

    /**
     * {field: 1} for ascending, {field: -1} for descending.
     *
     * @throws JsonProcessingException
     */
    @Test
    public void sort() throws JsonProcessingException {
        //返回排序后的第一个对象
        Drama drama = collection.findOne().orderBy("{_id:1}").as(Drama.class);
        writeJSON(drama);
        //返回排序后的对象集合
        cursor = collection.find().sort("{_id:-1}").as(Drama.class);
        iterate(cursor.iterator());
    }


    /**
     * skip : Skip <n> results.  limit: Limit result to <n> rows. Highly recommended if you need only a certain number of rows for best performance.
     *
     * @throws JsonProcessingException
     */
    @Test
    public void skipAndLimit() throws JsonProcessingException {
        cursor = collection.find().skip(20).as(Drama.class);
        iterate(cursor.iterator());
        System.out.println("-----------------------------------");
        cursor = collection.find().limit(10).as(Drama.class);
        iterate(cursor.iterator());
    }


    @Test
    public void update() throws JsonProcessingException {

//        collection.update(Oid.withOid("58d2300f4db0640c1a24f007")).with("{$set: {title: #,href:#,text:#,day:#}}", "行尸走肉 第七季 The Walking Dead Season 7 (2016)[已更新14集][每周一更新][中英]", "http://bbs.ncar.cc/thread-28877-1-1.html", "行尸走肉 第七季 The Walking Dead Season ", "monday");
        Drama drama = collection.findOne(Oid.withOid("58d2300f4db0640c1a24f007")).as(Drama.class);
        writeJSON(drama);

        //批量更新
        WriteResult writeResult = collection.update("{day: #}", "sunday").upsert().multi().with("{$set: {text:#} }", "update");
        //输出更新的总条数
        System.out.println(writeResult.getN());


    }

    /**
     * link: https://docs.mongodb.com/manual/reference/operator/meta/hint/
     * The $hint operator forces the query optimizer to use a specific index to fulfill the query. Specify the index either by the index name or by document.
     * Use $hint for testing query performance and indexing strategies.
     */
    @Test
    public void hint() throws JsonProcessingException {
        //查询条件中的字段必须在数据库中已经设置了索引，否则会报异常
        cursor = collection.find().hint("{ title : 1}").as(Drama.class);
        iterate(cursor.iterator());
    }


    /**
     * link:https://docs.mongodb.com/manual/reference/operator/query-modifier/
     * PS: 在mongo shell中自3.2版本后过时
     *
     * @throws JsonProcessingException
     */
    @Test
    public void queryModifiers() throws JsonProcessingException {
        cursor = collection.find().with(new QueryModifier() {
            public void modify(DBCursor dbCursor) {
                dbCursor.batchSize(10);
                dbCursor.addSpecial("$maxScan", 1);
            }
        }).as(Drama.class);
        iterate(cursor.iterator());
    }

    /**
     * 声明@MongoId注解后 在保存对象时id字段会自动复制
     */
    @Test
    public void save() throws JsonProcessingException {
        Drama drama = new Drama();
        drama.title = "zzzzzzzzzzzzzzzzzzzzz";
        drama.href = "httpppppppppppppp";
        drama.day = "sunday";
        drama.text = "this is a test case";
        collection.save(drama);
        Drama d = collection.findOne("{title:'zzzzzzzzzzzzzzzzzzzzz'}").as(Drama.class);
        writeJSON(d);
    }

    /**
     * link:https://docs.mongodb.com/manual/reference/method/db.runCommand/
     * 需要硬解析，不推荐使用
     */
    @Test
    public void runCommand() throws JsonProcessingException {
        DB db = new MongoClient(HOST, PORT).getDB(DB_NAME);//specify host and port and more options in new MongoClient() args, the constructor has multiple implementations.
        Jongo jongo = new Jongo(db);
        DBObject result = jongo.runCommand("{ 'find': #}", COLLECTION_NAME).map(new RawResultHandler<DBObject>());
        BasicDBObject cursor = (BasicDBObject) result.get("cursor");
        BasicDBList dramas = (BasicDBList) cursor.get("firstBatch");
        Iterator iterator = dramas.iterator();
        while (iterator.hasNext()) {
            writeJSON(iterator.next());//{"_id":{"timestamp":1490169871,"machineIdentifier":5091428,"processIdentifier":3098,"counter":2420743,"time":1490169871000,"timeSecond":1490169871,"date":1490169871000},"{title":"行尸走肉123123","title":"行尸走肉 第七季 The Walking Dead Season 7 (2016)[已更新14集][每周一更新][中英]","href":"http://bbs.ncar.cc/thread-28877-1-1.html","text":"行尸走肉 第七季 The Walking Dead Season ","day":"monday"}
        }
    }


    @Test
    public void distinct() {
        //获取collection的所有文档的title字段
        List<String> titles = collection.distinct("title").as(String.class);
        titles.forEach(title -> System.out.println(title));
    }

    /**
     * link:https://docs.mongodb.com/manual/reference/operator/aggregation/
     *
     * @throws JsonProcessingException
     */
    @Test
    public void aggregate() throws JsonProcessingException {
        Aggregate.ResultsIterator resultsIterator = collection.aggregate("{ $match:{day:#}}", "sunday").and("{ $limit: #}", 5).as(Drama.class);
        iterate(resultsIterator.iterator());
    }

    /**
     * 自定义resultHandler对返回的结果进行加工处理。
     * @return
     */
    private  ResultHandler<Drama> wrapHandler() {
        ResultHandler<Drama> handler = new ResultHandler<Drama>() {
            @Override
            public Drama map(DBObject dbObject) {
                Drama drama = null;
                try {
                    drama = objectMapper.readValue(dbObject.toString(), Drama.class);
                    drama.text = drama.text + " after custom handling";
                    //这里还可以根据实际业务进行一些其他处理操作
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return drama;
            }
        };
        return handler;
    }


    @Test
    public void handleResult() throws JsonProcessingException {
        cursor = collection.find().map(wrapHandler());
        iterate(cursor.iterator());
    }


}
