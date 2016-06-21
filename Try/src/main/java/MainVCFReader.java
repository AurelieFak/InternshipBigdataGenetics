import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

// TODO unzip vcf files b/gzip
// choice of database
// TODO donner de vrais noms aux tables
// TODO Tester close, open , message d'erreurs etc
// TODO javadoc
// TODO github
// TODO deux lectures ? une qui cree les tables en fonction des donnees et une autre unenfois les tables crees qui stocke
// TODO BATCH STATEMENT and prepared statements
//  TODO pour chaque ligne tous les champs presents dans la table INFO ne seront pas remplis, il faut faire un truc au cas ou ce serait vide
// TODO CLEAN/REMOVE TABLE
// TODO Genotype infos ? find a file with those
// TODO JOIN TABLES ? CROSS TABLE OPERATION

/*The header line names the 8 fixed, mandatory columns. These columns are as follows:
 determine the types

    #CHROM alphanumeric string
    POS int
    ID alphaN String
    REF String
    ALT
    QUAL
    FILTER
    INFO

If genotype data is present in the file, these are followed by a FORMAT column header, then an arbitrary number of sample IDs. The header line is tab-delimited.
	*/

public class MainVCFReader {

    public static void main(String[] args) {

        BufferedReader br = null;
        String vcfSplitBy = ""; //delimiteur
        int compteurLines = 0;
        int counter=1;
        int startingPoint = 0;
        boolean start;
        int compterInfo = 0;
        boolean info = false;
        int numberOfLinesToRead=300;
        DatabaseFunctions dataFunc =new DatabaseFunctions();
        LinkedList<Data> bigDataTable = new LinkedList<Data>();
        LinkedList <String> samples = new LinkedList<String>();
        ArrayList<String> tabInfo = new ArrayList<String>(); // Tous les champs qui permettront de creer la table info
        String tableName="Info"; // stop ces noms !
        String tableName2="Data";
        String keySpaceName="Gene";
        Tools tool = new Tools ();
       LinkedList<String> hasToBeFloatTable=new LinkedList<String>();
       LinkedList<String> hasToBeIntTable=new LinkedList<String>();
        LinkedList<String> dataL = new LinkedList<String>();

        Cluster cluster = Cluster.builder()
                .addContactPoints("127.0.0.1")
                .build();
        Session session = cluster.connect();

        dataFunc.insertIntoHastoBeAfLoat();
        dataFunc.insertIntoHasToBeInt();
        dataFunc.insertIntoDataFields(dataL);

        for (int i=0;i<hasToBeFloatTable.size();i++)
         System.out.println(hasToBeFloatTable.get(i));

        for (int i=0;i<hasToBeIntTable.size();i++)
            System.out.println(hasToBeIntTable.get(i));

        try {

            String sCurrentLine;

          // br = new BufferedReader(new FileReader("/home/molgenis/Downloads/VCFFiles/clinvar_20160405.vcf"));
            // nouveau fichier => nouvelle table, nouvelles colonnes etc
            // TODO give the name of the chromosome
            br = new BufferedReader(new FileReader("/home/molgenis/Downloads/VCFFiles/ALL.chrY.phase3_integrated_v2a.20130502.genotypes.vcf"));
           // br = new BufferedReader(new FileReader("/home/molgenis/example.vcf"));
            // TODO error message pour la lecture , of file doesnt exist
            // lire et inserer en meme temps dans la table

            while ((sCurrentLine = br.readLine()) != null ) // && compteurLines < numberOfLinesToRead
            {

                // TODO DO I try to detect the end of the file ?
                compteurLines++;
                counter++;
               // System.out.println(sCurrentLine);
                String str[] = sCurrentLine.split("\t"); // espace or tab be careful
                String fields[] = sCurrentLine.split("=");


                if (str[0].equals("#CHROM") && compteurLines < numberOfLinesToRead) {
                    start = true;
                    System.out.println("-------------------- Data lines start here-------------------------");
                    startingPoint = compteurLines; // regarder a quelle ligne se situe la header line
                }


                //if (compteurLines > startingPoint && compteurLines < numberOfLinesToRead && str.length >= 8)    // Data lines From staring point we can start storing
                    if (compteurLines > startingPoint && str.length >= 8)    // Data lines From staring point we can start storing
                {
                    //System.out.println("There are nb fields " + str.length);
                    //for (int i=0;i<str.length;i++)
                    //System.out.println(str[i]+" Je suis l'element numero*"+i+"  elemtn suivant");
                    Data data = new Data(str[0], str[1], str[2], str[3], str[4], str[5], str[6], str[7]);
                    bigDataTable.add(data); // still useful ?

                    // System.out.println(" Dataaaaaaaaaaaaaaaaa"+str[2]);

                   // for (int i=0;i<str.length;i++)
                     //System.out.println("CQLSTT " +i+ " ***    " + str[i]);

                  System.out.println("Insertion declaration   "+ dataFunc.InsertIntoData(tableName2,str,counter,dataL));
                    session.execute(dataFunc.InsertIntoData(tableName2,str,counter,dataL));

                // normalement pour inserer le reste des donnees dans format il devrait etre possible d'utiliser Data

               // for (int i=0;i<samples.size();i++)
                    //System.out.println(samples.get(i));

              //  System.out.println(" Il y a "+str.length);

                    String infoFields[] = str[7].split(";");
                   // String geneFields[]=str[8].split       // GT fields
                    // TODO Improvement, can we considere HashMap , key value
                    LinkedList<String> splitTableValue= new LinkedList<String>();
                    LinkedList<String> splitTableField= new LinkedList<String>();

                    for (int i = 0; i < infoFields.length; i++)

                    {
                        // Verifier la presence du motif


                        String[] split;
                        if (infoFields[i].indexOf('=') != -1) // contient =
                        {
                            split = infoFields[i].split("=");
                           // System.out.println("Ceci est mon champs" + split[0] + " Ceci est mon split " + split[1]);
                            // for example AF= 123
                            splitTableValue.add(split[1]);
                            splitTableField.add(split[0]);
                        }

                    }

                  //  System.out.println(" There are "+ splitTableField.size()+"   "+splitTableValue.size());

                    // pour tout le reste donc ceux qui n'ont pas le egal
                    // TODO Qu'est-ce que cela veut dire quand il n'y a pas de = ? Attention : tous les champs n'ont pas de =
                    // TODO peut-etre les enlever de la table

                    // Example for an INFO field: DP=154;MQ=52;H2. Keys without corresponding values are allowed in order to indicate group membership
                    //(e.g. H2 indicates the SNP is found in HapMap 2). It is not necessary to list all the properties that a site does NOT have, by e.g. H2=0
                   // for
                   // {
                        // splitTableField.add
                       // splitTableValue.add
                   // }

                   // System.out.println(" My big big chaine"+ dataFunc.insertIntoInfo(tableName,splitTableField,splitTableValue,keySpaceName,counter));

                     session.execute(dataFunc.insertIntoInfo(tableName,splitTableField,splitTableValue,keySpaceName,counter));

                   /* System.out.println (" PREPARE " + dataFunc.prepare(tableName,tabInfo,keySpaceName,splitTableField) );
                    PreparedStatement prepared = session.prepare(dataFunc.prepare(tableName,tabInfo,keySpaceName,splitTableField));
                    BoundStatement bound = prepared.bind(dataFunc.tobeBound(counter,splitTableField,splitTableValue));
                    session.execute(bound); */

                    // ABOUT FORMAT
                    // The columns are SAMPLES GT ETC
                    LinkedList<String> GT= new LinkedList<String>();
                    for (int i=9;i<str.length-2;i++)
                    {
                        GT.add(str[i]);
                    }

                }

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

    }

}

