import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;

import java.io.Serializable;


// Java VM Options: -Xms512m -Xmx1024m

public class Spark implements Serializable {

    public Spark() {
        // just an initialisation of SparkT Context
        SparkConf conf = new SparkConf()
                .setAppName("Simple Application") // App name
                .setMaster("local[*]") // We use a local cluster
                .set("spark.cassandra.connection.host", "127.0.0.1") // Cassandra host IP
                .set("spark.cleaner.ttl", "3600");

        JavaSparkContext sc = new JavaSparkContext(conf);

        CassandraSQLContext sqlContext = new CassandraSQLContext(sc.sc());

       // DataFrame df = sqlContext.sql("SELECT * FROM gene.infob");
         String query = "SELECT * FROM gene.info WHERE aa='G' "; // ancestral allele
        //String query = "SELECT * FROM gene.info WHERE ac=2 "; // AC allele count in genotypes, for each ALT allele, in the same order as listed
        // todo je ne peux pas faire d'operations de comparaisons ! range queries have to be done on the clustering key portion of the primary key

        DataFrame df = sqlContext.sql(query);
        Row[] rows = df.collect();

       // TODO JOIN Tables
        //TODO , sous quelle forme renvoyer les reponses ? // renvoyer une row ? utiliser le idu pour etabllir une correspondance entre les tables
        // TODO explorer les operateurs de Spark

        System.out.println("We have " + rows.length + " rows that satisfy the query " +query);

        for(Row row : rows) {
            int id = row.fieldIndex("idu");
            int idd = row.getInt(id);
           // String name = row.getString(id);
            System.out.println("ID " + idd);
        }


        sc.stop();
    }

    public static void main(String[] args) {
        new Spark();
    }


}
