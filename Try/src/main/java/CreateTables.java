import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Created by molgenis on 6/9/16.
 */
public class CreateTables {

    public static void main(String[] args) {

        BufferedReader br = null;
        String vcfSplitBy = ""; //delimiteur
        int compteurLines = 0;
        int counter=1;
        int startingPoint = 0;
        boolean start;
        int compterInfo = 0;
        boolean info = false;
        int numberOfLinesToRead=300; // even less
        DatabaseFunctions dataFunc =new DatabaseFunctions();
        LinkedList<Data> bigDataTable = new LinkedList<Data>();
        LinkedList <String> samples = new LinkedList<String>();
        ArrayList<String> tabInfo = new ArrayList<String>(); // Tous les champs qui permettront de creer la table info
        String tableName="Info";
        String tableName2="Data";
        String keySpaceName="Gene";
        Tools tool = new Tools ();
        int counterIndex=0;
        ArrayList<String> splitTableField = new ArrayList<String>();
        LinkedList<String> data = new LinkedList<String>();


        Cluster cluster = Cluster.builder()
                .addContactPoints("127.0.0.1")
                .build();
        Session session = cluster.connect();

        //session.execute(dataFunc.createKeySpace(keySpaceName));
        session.execute(dataFunc.createTableData(keySpaceName,tableName2));

        dataFunc.insertIntoHastoBeAfLoat();
        dataFunc.insertIntoHasToBeInt();
        dataFunc.insertIntoDataFields(data);

        try {

            String sCurrentLine;

            // br = new BufferedReader(new FileReader("/home/molgenis/Downloads/VCFFiles/clinvar_20160405.vcf"));
            // nouveau fichier => nouvelle table, nouvelles colonnes etc
            // TODO give the name of the chromosome
            br = new BufferedReader(new FileReader("/home/molgenis/Downloads/VCFFiles/ALL.chrY.phase3_integrated_v2a.20130502.genotypes.vcf"));
            // br = new BufferedReader(new FileReader("/home/molgenis/example.vcf"));

            // TODO error message FILE NOT FOUND
            // lire et inserer en meme temps dans la table

            // We don't have to read the entire file, only the headers to construct the tables

            while ((sCurrentLine = br.readLine()) != null && compteurLines< numberOfLinesToRead) // we don't need to read the all file, just the header lines matter to create the tables

            {
                compteurLines++;
                counter++;
                // System.out.println(sCurrentLine);
                String str[] = sCurrentLine.split("\t"); // espace or tab be careful
                String fields[] = sCurrentLine.split("=");

                // les stocker dans un tableau Info et les mettre dans une table

                if (fields[0].equals("##INFO") == true) { // Extract from FIELD INFO , regarder dans les metas lines pour voir combien de champs differents on peut trouver
                    compterInfo++;
                    // System.out.println(" INFOOOOOOOOOOO" + compterInfo);
                    String bb[] = sCurrentLine.split(",");   // 1. On delimite par la virgule
                    String aa = bb[0]; // catch the infofield
                    String cc[] = aa.split("ID="); // 2.On delimite par "ID="
                    //System.out.println(" Ceci est le premier champs d'une ligne info "+bb[0]); // il le fait de trop
                    //System.out.println(" Ceci est le champs Info a mettre dans une table "+cc[1]); // il le fait de trop
                    tabInfo.add(cc[1]); // on garde ce qu'il y a a droite du ID=

                }

                tool.eliminerDoublons(tabInfo);

                // TODO A MEDITER POUR LA STRUCTURE DE FORMAT
                // TODO TRUCS DE GENOTYPE

                http://www.tutorialspoint.com/cassandra/cassandra_cql_collections.htm

                if (str[0].equals("#CHROM") && str.length>8) // DETECTER FORMAT // il faut identifier la ligne
                { // FORMAT is there and FORMAT is the 9th

                    //session.execute(dataFunc);

                }

                if (compteurLines > startingPoint && str.length >= 8)    // Data lines From staring point we can start storing
                {
                    String infoFields[] = str[7].split(";");

                    for (int i = 0; i < infoFields.length; i++)

                    {

                        String[] split;
                        if (infoFields[i].indexOf('=') != -1) // contient =
                        {
                            split = infoFields[i].split("=");
                            splitTableField.add(split[0]);
                        }

                    }

                    tool.eliminerDoublons(splitTableField);

                }
                // we can distinguish two cases :
                // format is not there
                //format is there
                // we create the table according to this condition
                // select samples where GT = 0

                // il faut que je fasse correspondre GT au sample

            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        System.out.println(splitTableField);

        System.out.println(" LA TABLE INFO creation    "+ dataFunc.createTableInfo(tableName,splitTableField,keySpaceName));

        // session.execute(dataFunc.createTableInfo(tableName,tabInfo,keySpaceName));
        session.execute(dataFunc.createTableInfo(tableName,splitTableField,keySpaceName));

        // TODO index for Data too
        // Vu qu'on connait les champs on peut tricher

        for (int i=0;i<data.size();i++)
        {
            counterIndex=counterIndex++;
            //System.out.println("Index"+ dataFunc.createIndex(i,tabInfo,tableName));
            session.execute(dataFunc.createIndex(i, splitTableField, tableName));
        }

        if (counterIndex==splitTableField.size())
            System.out.println(" All indexes have been created");


        for (int i=0;i<splitTableField.size();i++)
        {
            counterIndex=counterIndex++;
            //System.out.println("Index"+ dataFunc.createIndex(i,tabInfo,tableName));
            session.execute(dataFunc.createIndex(i, splitTableField, tableName));
        }

        if (counterIndex==splitTableField.size())
            System.out.println(" All indexes have been created");

        String cqlStatement=" CREATE TABLE  IF NOT EXISTS Gene.format (GT varchar,samples varchar PRIMARY KEY);";
       // session.execute(cqlStatement);


    }

}
