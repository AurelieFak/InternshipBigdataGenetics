import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Created by molgenis on 5/23/16.
 */

// TODO table names lowercase/uppercase Consistency

public class DatabaseFunctions {
    // TODO maybe change the name
    // TODO Keyspace

    public LinkedList<String> hasToBeFloatTable=new LinkedList<String>();
    public LinkedList<String> hasToBeIntTable=new LinkedList<String>();


    public void insertIntoHastoBeAfLoat () {

        hasToBeFloatTable.add("af"); // Allele frequency
       // hasToBeFloatTable.add("ac");
        hasToBeFloatTable.add("amr_af");
        hasToBeFloatTable.add("afr_af");
        hasToBeFloatTable.add("eur_af");
        hasToBeFloatTable.add("sas_af");


        hasToBeFloatTable.add("rs");

    }
    // TODO INTEGER
    public void insertIntoHasToBeInt() {

        hasToBeIntTable.add("pos");

    }

    public String creationTable (String tableName){

        String cqlStt=" CREATE TABLE IF NOT EXISTS"; // TODO Keyspace and stuffs
        cqlStt+= tableName;

        return cqlStt;
        // OU session.execute(cqlStt);

    }

    public void insertionInTable (String tableName, LinkedList<String> fields, LinkedList<String> values) {

        String cqlStt=" INSERT INTO";
        cqlStt+= tableName;
        cqlStt += "VALUES ";
        // TODO KEEP GOING
    }

    public void clearTable (String tableName) {

    }

    public String createKeySpace ()
    {
        String cqlStatement = "CREATE KEYSPACE IF NOT EXISTS Gene WITH " +
                "replication = {'class':'SimpleStrategy','replication_factor':1}";
        return cqlStatement;
    }

    // TODO the names of the tables can come from the numer of the chromosome

    public String createTableDataD() {

        String cqlStatement2 = "CREATE TABLE IF NOT EXISTS Gene.dataC (" +
                " chrom varchar," +
                " pos int, " + // the cassandra table knows that pos is an int but not the datastructure
                " id varchar PRIMARY KEY, " + // does it have to be first in the creation of the table ?
                " ref varchar, " +
                " qual varchar, " +
                " filter varchar, " +
                " alt varchar, " +
                " info varchar" +
                ");";

        return cqlStatement2;
    }

    // TODO les fonctions peuvent etre plus generiques
    String tableName;

public String InsertIntoDataC (String []str)
{
    int last = str.length;
    String cqlStatement3 = "INSERT INTO Gene.dataC(chrom,pos,id,ref,qual,filter,alt,info) VALUES ('";
    for (int i = 0; i < str.length-1; i++) // 0 1 2 3 4 5 6  length = 8
    {
        cqlStatement3 += str[i] + "','";
    }
                // the last one
    cqlStatement3 += str[last-1];
   // System.out.println("CQLSTT" + str[last-1]);
    cqlStatement3 +=  "');";

   //System.out.println("CQLSTT" + cqlStatement3);

    return cqlStatement3;

}

    // Functions to change Varchar into Float or integer
    // Mostly for the Info table
    // TODO a tester sur autre fichier if hastobeAFloat works

    public String createTableInfo (String tableName,ArrayList<String> tabInfo)
    {
        // TODO eliminer doublons ?
        // Attention au 1 , il y avait dp en double debut et fin dans le fichier chrom Y  , why ?
        // alors que dp n'est present qu'a la fin des fields info dans l'entete
        boolean writtenOnce=false;
        LinkedList<String> lowerTableInfo=new LinkedList<String>();

        for(int k=0;k<tabInfo.size();k++)
            lowerTableInfo.add(tabInfo.get(k).toLowerCase());

        for(int i=0;i<lowerTableInfo.size();i++)
            System.out.println(" BOUHOU"+lowerTableInfo.get(i));

        String cqlstatement = "CREATE TABLE IF NOT EXISTS Gene.";
                cqlstatement +=tableName+" ("; // create table Info a a partir des lignes d'entete dans le fichier

        for (int i=0; i < lowerTableInfo.size()-1; i++)
        {

            for (int j=0;j<hasToBeFloatTable.size();j++)
            {
                //System.out.println("***"+lowerTableInfo.get(i)+"OOOO"+hasToBeFloatTable.get(j));

                if (lowerTableInfo.get(i).equals(hasToBeFloatTable.get(j)) && !writtenOnce)
                    // we write it once and we stop
                    // si il le trouve
                    // on continue a chercher jusqu'a ce qu'on le trouve ou pas
                {
                    Float f = Float.parseFloat(lowerTableInfo.get(i));
                    Float f1 = Float.valueOf(lowerTableInfo.get(i));
                    System.out.println("It's there " + lowerTableInfo.get(i) + "     " + hasToBeFloatTable.get(j));
                   // cqlstatement += lowerTableInfo.get(i)+ " float,";
                    cqlstatement += f+ " float,";
                    writtenOnce=true;

                }

                else if (!lowerTableInfo.get(i).equals(hasToBeFloatTable.get(j)) && !writtenOnce && j==hasToBeFloatTable.size()-1) // && boolTableFloatFamily.get(i)==false
                {
                    writtenOnce = true;
                    System.out.println("It's not there " + lowerTableInfo.get(i) + "     " + hasToBeFloatTable.get(j));
                    cqlstatement += lowerTableInfo.get(i) + " varchar,";
                }
            }

            writtenOnce=false;
        }

        // last field TODO WHICH ONE IS NAKES A GOOD PRIMARY KEY ?
        // TODO le dernier aussi peut-etre un float donc a modifier
        cqlstatement += lowerTableInfo.get(lowerTableInfo.size()-1) + " varchar PRIMARY KEY";
        cqlstatement +="); ";
        //System.out.println(cqlstatement);
        return cqlstatement;
    }


    public String createTableFormat() {
        // qui dit format dit samples  ? les samples doivent aussi etre dans la table
        // semi fix ? on ajoute les samples dynamiquement
        String cqlStatement5 = "CREATE TABLE FORMAT" // TODO la creer dynamiquement en fonction de ce qu'on lit dans l'entete du fichier
                + "GT varchar,"
                + "DP varchar,"
                + "FT varchar,"
                + "GL varchar,"
                + "GQ varchar,"
                + "HQ varchar"
                + ";)";
        return cqlStatement5;

    }


    public String insertIntoInfo (String tableName,LinkedList<String> splitTableField, LinkedList<String> splitTableValue)
    {

        String cqlStt = " INSERT INTO Gene.";
        cqlStt +=tableName+" (";
        // boucle pour iterer sur les champs
        for ( int k=0;k<splitTableField.size()-1;k++)
        {
            //cqlStt+="'";
            cqlStt+= splitTableField.get(k).toLowerCase();
            cqlStt+= ",";

        }
        //cqlStt+="'";
        cqlStt+=splitTableField.getLast().toLowerCase();
        //cqlStt+= "'";

        //TODO IS IT SILL USEFUL ?
        cqlStt+=") VALUES ("; // FORMAT DES VALUES COMPLIQUE A GERER
        // boucle pour iterer sur les valeurs
        for ( int k=0;k<splitTableValue.size()-1;k++) {

            if (splitTableValue.get(k).indexOf(":") !=-1 ) // contains :
            { // TODO regler le probleme des deux points
                System.out.println("That happens again");
                cqlStt+="'";
                cqlStt+= splitTableValue.get(k).replace(":","A");
                cqlStt+= "',";
                // System.out.println(cqlSttSoFar);
            }

            else if (splitTableValue.get(k).indexOf(",") !=-1 ) // contains ,
            { // TODO regler le probleme de la virgule
                System.out.println("That happens again");
                cqlStt+="'";
                cqlStt+= splitTableValue.get(k).replace(",","BB");
                cqlStt+= "',";
            }

            else
            {
                cqlStt+="'";
                cqlStt += splitTableValue.get(k);
                cqlStt += "',";
            }
        }

        if (splitTableValue.getLast().indexOf(",") !=-1 )  // TODO et si le dernier a aussi une virgule ou deux points ?
        {
            cqlStt+= splitTableValue.getLast().replace(",","BB");
        }

        else  if (splitTableValue.getLast().indexOf(":") !=-1 )  // TODO et si le dernier a aussi une virgule ou deux points ?
        {
            cqlStt+= splitTableValue.getLast().replace(":","BB");

        }

        else {
            cqlStt+="'";
            cqlStt+= splitTableValue.getLast();
            cqlStt+="'";
        }
        cqlStt+="); ";
        return cqlStt;
    }


  /*  public String creationIndex(ArrayList<String> tabInfo)
    {
        // TODO more infos :  to perform queries on fields different from the primary key we need index
        // for now I create an index for every column/field
        for (int i=0;i<tabInfo.size();i++)
        {
            String cqlStatement4 = " CREATE INDEX IF NOT EXISTS "
                    + tabInfo.get(i).toLowerCase() // indexname
                    + "a ON Gene.InfoB"
                    +"("
                    +tabInfo.get(i).toLowerCase()
                    +");";
            System.out.println(cqlStatement4);
        }

        return cql
    }*/

    // TODO to improve
    // https://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Session.prepare

 /*   public String batch () {
        Insert insertStatement = QueryBuilder.insertInto("Gene.InfoB");
        insertStatement.value("id", QueryBuilder.bindMarker())
                .value("chrom", QueryBuilder.bindMarker())
                .value("pos", QueryBuilder.bindMarker())
                .value("alt", QueryBuilder.bindMarker())
                .value("ref", QueryBuilder.bindMarker())
                .value("qual", QueryBuilder.bindMarker())
        ;
       // PreparedStatement ps = session.prepare(insertStatement.toString());

        BatchStatement batch = new BatchStatement();
        for ( Data data : datas)
        {
            //batch.add(ps.bind(roadtrip.getId(),
                    data.getChrom(),
                    data.getPos(),
                    data.getAlt(),
                    data.getQual(),
                    data.getRef()
            ));
        }
        // session.execute(batch);
    } */

    // http://www.planetcassandra.org/getting-started-with-apache-cassandra-and-java-part-2/?utm_content=buffer998b6&utm_medium=social&utm_source=plus.google.com&utm_campaign=buffer

    public void PreparedStateM (LinkedList<String> splitTableValue)
    {

        // Insert one record into the users table
        // PreparedStatement statement = session.prepare( "INSERT INTO InfoY                  VALUES (?,?,?,?,?);");

      //  BoundStatement boundStatement = new BoundStatement(statement);

      //  session.execute(boundStatement.bind("Jones", 35, "Austin", "bob@example.com", "Bob"));

       // for (int i=0;i<splitTableValue.size();i++)
           // splitTableValue.get(i);

    }

}
