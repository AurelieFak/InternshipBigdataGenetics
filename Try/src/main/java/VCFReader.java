import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

// TODO unzip vcf files b/gzip
// choice of database
// TODO donner de vrais noms aux tables
// TODO parser en int and other types
// TODO Tester close, open , message d'erreurs etc
// TODO javadoc
// TODO github
// TODO deux lectures ? une qui cree les tables en fonction des donnees et une autre unenfois les tables crees qui stocke
// TODO BATCH STATEMENT and prepared statements
//  TODO pour chaque ligne tous les champs presents dans la table INFO ne seront pas remplis, il faut faire un truc au cas ou ce serait vide
// TODO CLEAN/REMOVE TABLE
// TODO Genotype infos ? find a file with those
// TODO JOIN TABLES ? CROSS TABLE OPERATION
// TODO BIIIIIIIIIIIIIIIIG PROBLEM D'ORDRE, LECTURE ET CREATION DES TABLES ET ENSUITE INSERTION

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

public class VCFReader {

    public static void main(String[] args) {

        BufferedReader br = null;
        String vcfSplitBy = ""; //delimiteur
        int compteurLines = 0;
        int startingPoint = 0;
        boolean start;
        int compterInfo = 0;
        boolean info = false;
        int numberOfLinesToRead=200;
        DatabaseFunctions creationTable=new DatabaseFunctions();
        LinkedList<Data> bigDataTable = new LinkedList<Data>();
        ArrayList<String> tabInfo = new ArrayList<String>(); // Tous les champs qui permettront de creer la table info
        String tableName="InfoB";
        String keySpaceName;

        Cluster cluster = Cluster.builder()
                .addContactPoints("127.0.0.1")
                .build();
        Session session = cluster.connect();

        session.execute(creationTable.createKeySpace());
        session.execute(creationTable.createTableDataD());

        try {

            String sCurrentLine;

            br = new BufferedReader(new FileReader("/home/molgenis/Downloads/VCFFiles/clinvar_20160405.vcf"));
            // nouveau fichier => nouvelle table, nouvelles colonnes etc
            // TODO give the name of the chromosome
            //br = new BufferedReader(new FileReader("/home/molgenis/Downloads/VCFFiles/ALL.chrY.phase3_integrated_v2a.20130502.genotypes.vcf"));
           // br = new BufferedReader(new FileReader("/home/molgenis/example.vcf"));

            // TODO error message
            // lire et inserer en meme temps dans la table

            // Pas oblige de lire le fichier en entier

            while ((sCurrentLine = br.readLine()) != null ) // && compteurLines < numberOfLinesToRead
            {
                compteurLines++;
               // System.out.println(sCurrentLine);
                String str[] = sCurrentLine.split("\t"); // espace or tab be careful
                String fields[] = sCurrentLine.split("=");


                if (fields[0].equals("##INFO") == true) { // Extract from FIELD INFO , regarder dans les metas lines pour voir combien de champs differents on peut trouver
                    compterInfo++;
                   // System.out.println(" INFOOOOOOOOOOO" + compterInfo);
                    String bb[] = sCurrentLine.split(",");   // 1. On delimite par la virgule
                    String aa = bb[0]; // catch the infofield
                    String cc[] = aa.split("ID="); // 2.On delimite par "ID="
                    //System.out.println(" Ceci est le premier champs d'une ligne info "+bb[0]); // il le fait de trop
                    //System.out.println(" Ceci est le champs Info a mettre dans une table "+cc[1]); // il le fait de trop
                    tabInfo.add(cc[1]); // on garde ce qu'il y a a droite du ID=
                    // les stocker dans un tableau Info et les mettre dans une table
                }

               // for (int i=0;i<tabInfo.size();i++)
                   // tabInfo.get(i).toLowerCase();


                //System.out.println("Il y a"+ compterInfo+"champs Info");
                // il faut identifier tous les champs INFOS et creer la table a partir de ces champs
                // il y a deux etapes ; LIRE LE FICHIER et extraire les infos nessecaires ET ENSUITE CREER LA TABLE A PARTIR DES CHAMPS DE INFO


                if (str[0].equals("#CHROM") && compteurLines < numberOfLinesToRead) {
                    start = true;
                    System.out.println("-------------------- Data lines start here-------------------------");
                    startingPoint = compteurLines; // regarder a quelle ligne se situe la header line
                }


               // System.out.println(" Il y a "+str.length);

                if (compteurLines > startingPoint && compteurLines < numberOfLinesToRead && str.length >= 8)    // Data lines From staring point we can start storing
                {
                    //System.out.println("There are nb fields " + str.length);
                    //for (int i=0;i<str.length;i++)
                    //System.out.println(str[i]+" Je suis l'element numero*"+i+"  elemtn suivant");
                    Data data = new Data(str[0], str[1], str[2], str[3], str[4], str[5], str[6], str[7]);
                    bigDataTable.add(data);
                    // System.out.println(" Dataaaaaaaaaaaaaaaaa"+str[2]);

                    // TODO Faire toutes les insertions BATCH
                    // TODO a mettre ailleurs
                    // bebe version => creer un tableau big tableau data et puis inserer dans la DB , une etape de trop
                    // TODO a modifier, comment faire une boucle d'insertion ?
                    // Ce n'est pas au bon endroit
                   // for (int i=0;i<str.length;i++)
                     //System.out.println("CQLSTT " +i+ " ***    " + str[i]);

                  //  System.out.println("Insertion declaration   "+ creationTable.InsertIntoDataC(str));
                   // session.execute(creationTable.InsertIntoDataC(str));

                }


                if (compteurLines > startingPoint && compteurLines < numberOfLinesToRead && str.length >= 8)
                {

                    System.out.println("Pour chaque ligne");
                    String infoFields[] = str[7].split(";");
                   // String geneFields[]=str[8].split       // GT fields

                    LinkedList<String> splitTableValue= new LinkedList<String>();
                    LinkedList<String> splitTableField= new LinkedList<String>();
                    for (int i = 0; i < infoFields.length; i++)

                    {
                        // Verifier la presence du motif
                        // TODO Qu'est-ce que cela veut dire quand il n'y a pas de = ? Attention : tous les champs n'ont pas de =

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

                    // Je veux le nombre de champs par ligne  // both must be the same ofc
                    //System.out.println(" There are "+ splitTableField.size()+"   "+splitTableValue.size());

                    // TODO session,prepare()
                   // query=

                    System.out.println (" Les champs infos derives de l'entete sont :");
                   // for (int i=0;i<tabInfo.size();i++)
                       // System.out.println("@@@"+tabInfo.get(i));
                    //System.out.println(tabInfo.get(tabInfo.size()-1));

                   // System.out.println(" Le dernier est"+ splitTableValue.getLast());

                    System.out.println(" My big big chaine"+creationTable.insertIntoInfo(tableName,splitTableField,splitTableValue));
                   // session.execute(creationTable.insertIntoInfo(tableName,splitTableField,splitTableValue));

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
            //for(int i=0;i<tabInfo.size();i++)
               // System.out.println("Taaaaaaaaaaaab   "+i+"BOUUUUUUUUUU    "+tabInfo.get(i));

        creationTable.insertIntoHastoBeAfLoat();
        System.out.println(" LA TABLE INFO creation    "+ creationTable.createTableInfo(tableName,tabInfo));
        session.execute(creationTable.createTableInfo(tableName,tabInfo));

        // TODO more infos :  to perform queries we need index
        // for now I create an index for every column/field     as much indexes as columns
        // Attention +1
        //
        for (int i=1;i<tabInfo.size();i++) {
            String cqlStatement4 = " CREATE INDEX IF NOT EXISTS "
                    + tabInfo.get(i).toLowerCase() // indexname
                    + "a ON Gene.InfoY"
                    +"("
                    +tabInfo.get(i).toLowerCase()
                    +");";
            System.out.println(cqlStatement4);
           // session.execute(cqlStatement4);
        }

    }

}

