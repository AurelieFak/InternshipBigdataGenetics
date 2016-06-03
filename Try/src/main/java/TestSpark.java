import com.google.common.base.Objects;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;


// Java VM Options: -Xms512m -Xmx1024m

public class TestSpark implements Serializable {

    public TestSpark() {
        // just an initialisation of Spark Context
        SparkConf conf = new SparkConf()
                .setAppName("Simple Application") // App name
                .setMaster("local[*]") // We use a local cluster
                .set("spark.cassandra.connection.host", "127.0.0.1") // Cassandra host IP
                .set("spark.cleaner.ttl", "3600");

        JavaSparkContext sc = new JavaSparkContext(conf);


        CassandraSQLContext sqlContext = new CassandraSQLContext(sc.sc());

       // DataFrame df = sqlContext.sql("SELECT * FROM gene.infob");
         DataFrame df = sqlContext.sql("SELECT * FROM gene.infob WHERE dbsnpbuildid='142' ");
       // DataFrame df = sqlContext.sql("SELECT COUNT (*) FROM gene.infob WHERE dbsnpbuildid='142' ");
       // dbSNPBuildID=142
        Row[] rows = df.collect();

       // TODO JOIN Tables

        System.out.println(rows.length);

        for(Row row : rows) {
            int id = row.fieldIndex("rs");
            String name = row.getString(id);
            System.out.println(name);
        }


        sc.stop();
    }

    public static void main(String[] args) {
        new TestSpark();
    }


}
