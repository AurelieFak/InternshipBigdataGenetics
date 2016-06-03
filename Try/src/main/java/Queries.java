/**
 * Created by molgenis on 5/23/16.
 */
public class Queries {

    // Table that I have by now :
    // DataC
    // InfoB
    // format

    // TODO find better names and consistency
    // Tant que mes champs infos ne sont pas des Integer je ne pourrais pas les comparer


    String query1 = "SELECT * FROM Gene.InfoB;";
    String query2 = "SELECT * FROM Gene.InfoB WHERE AF < 0.01;";
    String query3 = "SELECT * FROM Gene.InfoB WHERE AC=1;" ;

    public void executeQuery (String queryNumber) {
        //session.execute(queryNumber);
    }
}
