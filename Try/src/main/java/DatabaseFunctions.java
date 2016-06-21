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
// TODO the names of the tables can come from the number of the chromosome

public class DatabaseFunctions {

    // TODO NEED TO CHECK IF THE IDU IS THE SAME

    public LinkedList<String> hasToBeFloatTable=new LinkedList<String>();
    public LinkedList<String> hasToBeIntTable=new LinkedList<String>();


    public void insertIntoHastoBeAfLoat () {

        // others fields can be added

        hasToBeFloatTable.add("af"); // Allele frequency
        hasToBeFloatTable.add("ac");
        hasToBeFloatTable.add("amr_af");
        hasToBeFloatTable.add("afr_af");
        hasToBeFloatTable.add("eur_af");
        hasToBeFloatTable.add("sas_af");
        hasToBeFloatTable.add("eas_af");

    }


    public void insertIntoHasToBeInt() {
         // todo to call lors de la creation et de l'insertion
        // others fields can be added

        hasToBeIntTable.add("pos");
        hasToBeIntTable.add("an");
        hasToBeIntTable.add("dp");
        hasToBeIntTable.add("ns");

    }

    public void insertIntoDataFields(LinkedList<String> data) {

        data.add("chrom");
        data.add("pos"); // is an int
        data.add("id");
        data.add("ref");
        data.add("qual");
        data.add("filter");
        data.add("alt");
        data.add("info");

        //System.out.println(data);

    }

    // function to create a keySpace

    public String createKeySpace (String keySpaceName)
    {
        String cqlStatement = "CREATE KEYSPACE IF NOT EXISTS " +
                keySpaceName+
                "WITH " +
                "replication = {'class':'SimpleStrategy','replication_factor':1}";
        return cqlStatement;
    }

    // function to create a table

    public String creationTable (String tableName){

        String cqlStt=" CREATE TABLE IF NOT EXISTS"; // TODO Keyspace and stuffs
        cqlStt+= tableName;

        return cqlStt;

    }

    public String createTableData(String keySpaceName, String tableName) {

        String cqlStatement2 = "CREATE TABLE IF NOT EXISTS "+
                keySpaceName+
                "."+
                tableName +
                " (" +
                "idu int PRIMARY KEY," +
                " chrom varchar," +
                " pos int, " + // the cassandra table knows that pos is an int but not the datastructure
                " id varchar, " + // does it have to be first in the creation of the table ?
                " ref varchar, " +
                " qual varchar, " +
                " filter varchar, " +
                " alt varchar, " +
                " info varchar" +
                ");";

        return cqlStatement2;
    }

    public String createTableInfo (String tableName,ArrayList<String> tabInfo,String keySpaceName )
    // TODO no NULL
            // soit je remove les collones je triche
            // if value == null, " GM "
            // faut juste constuire la table a partir de splitFieldLower
    {

        boolean writtenOnce=false;
        boolean writtenOnce2=false;
        LinkedList<String> lowerTableInfo=new LinkedList<String>();

        for(int k=0;k<tabInfo.size();k++)
            lowerTableInfo.add(tabInfo.get(k).toLowerCase());

        // for(int i=0;i<lowerTableInfo.size();i++)
        // System.out.println(" BOUHOU"+lowerTableInfo.get(i));

        String cqlstatement = "CREATE TABLE IF NOT EXISTS ";
        cqlstatement+=keySpaceName+".";
        cqlstatement +=tableName+" ("; // create table Info a a partir des lignes d'entete dans le fichier
        cqlstatement+="idu int PRIMARY KEY,";

        for (int i=0; i < lowerTableInfo.size()-1; i++)
        {
            for (int j=0;j<hasToBeFloatTable.size();j++)
            {
                //  for (int k=0;k<hasToBeIntTable.size();k++) TODO triple boucle ? iterer sur k pour les Int

                if (lowerTableInfo.get(i).equals(hasToBeFloatTable.get(j)) && !writtenOnce) // les champs float n'ont pas de guillemets
                // we write it once and we stop          si il le trouve     on continue a chercher jusqu'a ce qu'on le trouve ou pas
                {
                    // System.out.println("It's there " + lowerTableInfo.get(i) + "     " + hasToBeFloatTable.get(j));
                    cqlstatement += lowerTableInfo.get(i)+ " float,";
                    writtenOnce=true;

                }

                else if (!lowerTableInfo.get(i).equals(hasToBeFloatTable.get(j)) && !writtenOnce && j==hasToBeFloatTable.size()-1) // && boolTableFloatFamily.get(i)==false
                {
                    writtenOnce = true;
                    // System.out.println("It's not there " + lowerTableInfo.get(i) + "     " + hasToBeFloatTable.get(j));
                    cqlstatement += lowerTableInfo.get(i) + " varchar,";
                }

          /*     if (lowerTableInfo.get(i).equals(hasToBeIntTable.get(k)) && !writtenOnce) // les champs int n'ont pas de guillemets
                {
                    cqlstatement += lowerTableInfo.get(i)+ " int,";
                    writtenOnce=true;

                }

                else if (!lowerTableInfo.get(i).equals(hasToBeFloatTable.get(j)) && !lowerTableInfo.get(i).equals(hasToBeIntTable.get(k))
                        && !writtenOnce && j==hasToBeFloatTable.size()-1) // && boolTableFloatFamily.get(i)==false
                {
                    writtenOnce = true;
                    // System.out.println("It's not there " + lowerTableInfo.get(i) + "     " + hasToBeFloatTable.get(j));
                    cqlstatement += lowerTableInfo.get(i) + " varchar,";
                } */
            }

            writtenOnce=false;
        }
        // The last one
        for (int j=0;j<hasToBeFloatTable.size();j++)
        {

            if (lowerTableInfo.get(lowerTableInfo.size()-1).equals(hasToBeFloatTable.get(j)) && !writtenOnce2) {
                writtenOnce2=true;
                cqlstatement += lowerTableInfo.get(lowerTableInfo.size()-1) + " float";
            }


            else if (!lowerTableInfo.get(lowerTableInfo.size()-1).equals(hasToBeFloatTable.get(j)) && !writtenOnce2 && j==hasToBeFloatTable.size()-1){
                writtenOnce2=true;
                cqlstatement += lowerTableInfo.get(lowerTableInfo.size()-1) + " varchar";
            }

            writtenOnce2=false;
        }

        cqlstatement +="); ";
        //System.out.println(cqlstatement);
        return cqlstatement;
    }


    public String createTableFormat() {
        // qui dit format dit samples  ? les samples doivent aussi etre dans la table
        // semi fix ? on ajoute les samples dynamiquement
        String cqlStatement = "CREATE TABLE FORMAT" // TODO la creer dynamiquement en fonction de ce qu'on lit dans l'entete du fichier
                + "Samples varchar,"
                + "GT varchar,"
                + "DP varchar,"
                + "FT varchar,"
                + "GL varchar,"
                + "GQ varchar,"
                + "HQ varchar"
                + ";)";
        return cqlStatement;

    }

    public String createTableSamples (String []str, LinkedList<String> samples) // the samples are supposed to be the columns
    {
        String cqlStatement="CREATE TABLE IF NOT EXISTS Gene.format2( ";
        System.out.println(" IL y a " + str.length + "champs");
        for (int i=9;i<str.length-2;i++)
        {
            samples.add(str[i]); // aurais=je pu inserer directement sans un tableau intermediaire, maybe too heavy
            cqlStatement+= str[i];
            cqlStatement+=" varchar,";
            //System.out.println(str[i]);
        }
        // and the last one
        cqlStatement+=str[str.length-1];
        cqlStatement+=" varchar PRIMARY KEY);";

        System.out.println("7th one"+cqlStatement);

        return cqlStatement;
    }

    // functions to insert into a table
    public void insertionInTable (String tableName, LinkedList<String> fields, LinkedList<String> values) {

        String cqlStt=" INSERT INTO";
        cqlStt+= tableName;
        cqlStt += "VALUES ";
        // TODO KEEP GOING
    }

    public String InsertIntoData (String tableName, String []str,int counter,LinkedList<String> data)
    {
        boolean writtenOnce=false;


        //for (int i=0;i<str.length;i++)
        // System.out.println(str[i]);

        // System.out.println(hasToBeIntTable);

        int last = str.length; // on ne prend pas tout pour l'instant


        String cqlStatement = "INSERT INTO Gene." +
                tableName +
                "(idu,chrom,pos,id,ref,qual,filter,alt,info) VALUES (";
        cqlStatement+=counter+",";
        for (int i=0; i < 7; i++) // 0 1 2 3 4 5 6  length = 8
        {
            for(int j=0;j<hasToBeIntTable.size();j++)
            {
                if (data.get(i).equals(hasToBeIntTable.get(j)) && !writtenOnce) // ne pas mettre de guillemets si c'est un int
                {
                    writtenOnce=true;
                    cqlStatement += str[i] + ",";
                }

                else if (!data.get(i).equals(hasToBeIntTable.get(j)) && !writtenOnce )
                {
                    writtenOnce=true;
                    cqlStatement += "'"+str[i]+"',";
                }
            }

            writtenOnce=false;

        }
        // the last one
        cqlStatement += "'"+str[7]+"'";
        // System.out.println("CQLSTT" + str[last-1]);
        cqlStatement +=  ");";

        //System.out.println("CQLSTT" + cqlStatement3);

        return cqlStatement;

    }

    public String insertIntoInfo (String tableName,LinkedList<String> splitTableField, LinkedList<String> splitTableValue, String keySpaceName, int counter) {
        // int counter=0;


        System.out.println(" Taille de la table " + splitTableField.size()+"    "+splitTableValue);

        boolean writtenOnce = false;
        boolean writtenOnce2=false;
        LinkedList<String> splitFieldLower = new LinkedList<String>();

        for (int k = 0; k < splitTableField.size(); k++)
            splitFieldLower.add(splitTableField.get(k).toLowerCase());

        String cqlStt = "INSERT INTO ";
        cqlStt+=keySpaceName+".";// FIELDS
        cqlStt += tableName + " (";
        // boucle pour iterer sur les champs
        cqlStt+="idu, ";
        for (int k = 0; k < splitTableField.size()-1; k++) {
            //cqlStt+="'";
            cqlStt += splitTableField.get(k).toLowerCase();
            cqlStt += ",";

        }
        //cqlStt+="'";
        cqlStt += splitTableField.getLast().toLowerCase();
        //cqlStt+= "'";

        cqlStt += ") VALUES ("; // FORMAT DES VALUES COMPLIQUE A GERER
        // boucle pour iterer sur les valeurs
        cqlStt+=counter+", ";
        for (int k = 0; k < splitFieldLower.size()-1; k++)
        {
            for (int i = 0; i < hasToBeFloatTable.size(); i++)
            {
                if (splitFieldLower.get(k).equals(hasToBeFloatTable.get(i)) && !writtenOnce) // TODO MOFIDY THE CONDITION
                {
                    writtenOnce = true;
                    cqlStt += splitTableValue.get(k); // it's not a string so I could I add it in the ?
                    cqlStt+=",";

                }

                /*if (splitFieldLower.get(k).equals(hasToBeIntTable.get(i)) && !writtenOnce) // TODO MOFIDY THE CONDITION
                {
                    writtenOnce = true;
                    cqlStt += splitTableValue.get(k); // it's not a string so I could I add it in the ?
                    cqlStt+=",";

                }*/

                else if (!splitFieldLower.get(k).equals(hasToBeFloatTable.get(i)) && !writtenOnce && i == hasToBeFloatTable.size()-1)
                {
                    writtenOnce = true;
                    cqlStt += "'";
                    cqlStt += splitTableValue.get(k);
                    cqlStt += "',";
                }

            }

            writtenOnce=false;
        }

        for (int i = 0; i < hasToBeFloatTable.size(); i++)
        {
            if (splitFieldLower.getLast().equals(hasToBeFloatTable.get(i)) && !writtenOnce2 )
            {
                writtenOnce2=true;
                cqlStt += splitTableValue.getLast();
            }

            else if (!splitFieldLower.getLast().equals(hasToBeFloatTable.get(i)) && !writtenOnce2 && i == hasToBeFloatTable.size()-1 )
            {
                writtenOnce2 = true;
                cqlStt += "'";
                cqlStt += splitTableValue.getLast();
                cqlStt += "'";
            }

            writtenOnce2=false;
        }


        cqlStt += "); ";
        return cqlStt;

    }

    // function to remove a table

    public String removeTable (String keySpaceName, String tableName) {

        String cqlStatement = " drop table" +
                keySpaceName+
                "." +
                tableName+
                ",";

        return cqlStatement;
    }


    // function to remove all tables

    public String removeAllTables () // list of tables ?
    {
        String cqlStatement=" ";
        return cqlStatement;
    }

    public String prepare(String tableName, ArrayList<String> tabInfo, String keySpaceName, LinkedList<String> splitTableField) { // Needed for all insert


        String cqlStt = "INSERT INTO ";
        cqlStt+=keySpaceName+".";// FIELDS
        cqlStt += tableName + " (";
        // boucle pour iterer sur les champs
        cqlStt+="idu, ";
        for (int k = 0; k <splitTableField.size()-1; k++) {
            //cqlStt+="'";
            cqlStt += splitTableField.get(k).toLowerCase();
            cqlStt += ",";

        }

        cqlStt += splitTableField.getLast().toLowerCase();

        cqlStt += ") VALUES (";
        cqlStt+="?,";
        for (int k = 0; k <splitTableField.size()-1; k++)
            cqlStt += "?,";
        cqlStt+="?)";

        return cqlStt;
    }

    public String tobeBound (int counter, LinkedList<String> splitTableField,LinkedList<String> splitTableValue, LinkedList<String> hasToBeFloatTable) {

        boolean writtenOnce = false;
        LinkedList<String> splitFieldLower = new LinkedList<String>();

        for (int k = 0; k < splitTableField.size(); k++)
            splitFieldLower.add(splitTableField.get(k).toLowerCase());

        String cqlStt =counter+", ";

        for (int k = 0; k < splitFieldLower.size()-1; k++)
        {
            for (int i = 0; i < hasToBeFloatTable.size(); i++)
            {
                if (splitFieldLower.get(k).equals(hasToBeFloatTable.get(i)) && !writtenOnce) // TODO MOFIDY THE CONDITION
                {
                    writtenOnce = true;
                    cqlStt += splitTableValue.get(k); // it's not a string so I could I add it in the ?
                    cqlStt+=",";

                } else if (!splitFieldLower.get(k).equals(hasToBeFloatTable.get(i)) && !writtenOnce && i == hasToBeFloatTable.size()-1)
                {
                    writtenOnce = true;
                    cqlStt += "'";
                    cqlStt += splitTableValue.get(k);
                    cqlStt += "',";
                }

            }

            writtenOnce=false;
        }

        cqlStt += "'";
        cqlStt += splitTableValue.getLast();
        cqlStt += "'";

        cqlStt += "); ";
        return cqlStt;
    }

    public String InsertIntoFormat (LinkedList<String> samples)
    {
        String cqlStatement=" INSERT INFO Gene.format VALUES( ";

        //for(int i=0;i<samples.size();i++)

            return cqlStatement;
    }

    // Function to insert into the Info Table
    // TODO deal with and the equal is not there HYPER IMPORTANT



    // To perform queries on fields other than the primary key we need to index them

    public String createIndex(int i,ArrayList<String> tabInfo,String tableName)
    {
       ArrayList<String> indexList = new ArrayList<String>();

        // for now I create an index for every column/field     as much indexes as columns
        // why is end double ?

        String cqlStatement= " CREATE INDEX IF NOT EXISTS "
                    + tabInfo.get(i).toLowerCase() // indexname
                    + "Index ON Gene."
                    +tableName
                    +"("
                    +tabInfo.get(i).toLowerCase()
                    +");";

             System.out.println(cqlStatement);

        return cqlStatement;
    }

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
