package cn.imooc.bigdata.sparkestag.etl.es;

import cn.imooc.bigdata.sparkestag.support.SparkUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.io.Serializable;
import java.util.Map;

/**
 * @author bywind
 * spark 操作 ES
 */
public class EsDemo {

    public static void main(String[] args) {

        JavaSparkContext jsc = SparkUtils.getJSC4Es(true);
//        List<User> list = new ArrayList<>();
//        list.add(new User("Jack",18));
//        list.add(new User("Eric",20));
//        JavaRDD<User> userJavaRDD = jsc.parallelize(list);
//        JavaEsSpark.saveToEs(userJavaRDD,"/user/_doc");

//        JavaPairRDD<String, Map<String, Object>> pairRDD = JavaEsSpark.esRDD(jsc, "/user/_doc");
//        Map<String, Map<String, Object>> stringMapMap = pairRDD.collectAsMap();
//        System.out.println(stringMapMap);
//        JavaRDD<User> rdd = pairRDD.map(new Function<Tuple2<String, Map<String, Object>>, User>() {
//            @Override
//            public User call(Tuple2<String, Map<String, Object>> v1) throws Exception {
//                User user = new User();
//                BeanUtils.populate(user, v1._2());
//                return user;
//            }
//        });
//
//        List<User> collect = rdd.collect();
//        System.out.println(collect);

        String query = "{\"query\":{\"bool\":{\"should\":[{\"match\":{\"name\":\"Eric\"}},{\"range\":{\"FIELD\":{\"gte\":30,\"lte\":40}}}]}}}";
        JavaPairRDD<String, String> pairRDD = JavaEsSpark.esJsonRDD(jsc, "/user/_doc", query);
        Map<String, String> stringStringMap = pairRDD.collectAsMap();
        System.out.println(stringStringMap);

    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class User implements Serializable {
        private String name;
        private Integer age;
    }


}


