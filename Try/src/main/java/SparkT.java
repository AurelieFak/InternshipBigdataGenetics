//import com.datastax.spark.connector.CassandraJavaUtil;
import com.datastax.spark.connector.CassandraRow;
//import com.datastax.spark.connector.SparkContextJavaFunctions;
import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
//import static com.datastax.spark.connector.CassandraJavaUtil.javaFunctions;

/**
 * Created by molgenis on 5/27/16.
 */
public class SparkT {

    // reading data from Cassandra into RDD and writing RDD to Cassandra.

     /* Distinction between  :
    SparkConf
            JavaSparkContext
            JavaSQLContext
            SQL Context
            CassandraSQLContext
             Cassandra Connector*/

    /*
    JavaRDD;
    CREATE RDD
    CassandraSQLContext csc = new CassandraSQLContext(sqlContext);
    */


    public static void main(String[] args)
    {
        // Create the spark context.
          SparkConf conf = new SparkConf().setAppName("QueriesGene");
          JavaSparkContext sc = new JavaSparkContext(conf);

      //JavaSQLContext sqlContext = new JavaSQLContext(sc);

        // SQLContext sqlContext = new SQLContext(sc);
        //
        // CassandraSQLContext cassandraContext = new CassandraSQLContext(conf);
         CassandraConnector connector = CassandraConnector.apply(sc.getConf());

        // conf.setMaster(args[0]);
        // conf.set("spark.cassandra.connection.host", args[1]); // args 1 ?


        // Access to Cassandra
        // https://www.infoq.com/fr/articles/rdd-spark-datastax

       SparkConf conf1 = new SparkConf(true)
                .setMaster("local")
                .setAppName("DatastaxTests")
                .set("spark.executor.memory", "1g")
                .set("spark.cassandra.connection.host", "localhost")
                .set("spark.cassandra.connection.native.port", "9142")
                .set("spark.cassandra.connection.rpc.port", "9171");
        SparkContext ctx = new SparkContext(conf1);


        // ----------------------------------------------------------------------------------------

     /*   JavaPairRDD<Integer, Data> dataRDD =
                javaFunctions(sc).cassandraTable("Gene", "dataC").keyBy(new Function< Integer, Data >() {
                    @Override
                    public Integer call(Data data) throws Exception {
                        return data.getId();
                    }

                });


        SparkContextJavaFunctions functions = javaFunctions(ctx);

        JavaRDD<CassandraRow> rdd = functions.cassandraTable("Gene", "infoB");
        rdd.cache();
        JavaPairRDD test = rdd.groupBy(new Function<CassandraRow, String>() {
            @Override
            public Object call(Object o) throws Exception {
                return null;
            }
        }); */

        // https://docs.datastax.com/en/datastax_enterprise/4.6/datastax_enterprise/spark/sparkSqlJava.html
        // http://www.programcreek.com/java-api-examples/index.php?api=com.datastax.spark.connector.cql.CassandraConnector


        //JavaPairRDD<Integer,Data> rdd = functions.cassandraTable("Gene", "infoB").keyBy(new Function<Data, Integer>() {})


       // http://blog.zenika.com/2015/02/23/spark-et-cassandra/   GOOD ONE



    }

}
