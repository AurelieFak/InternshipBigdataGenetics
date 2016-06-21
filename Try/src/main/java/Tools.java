import java.util.ArrayList;

/**
 * Created by molgenis on 6/7/16.
 */
public class Tools {

    public void eliminerDoublons (ArrayList<String> tabInfo)
    {
         for ( int i=0;i<tabInfo.size();i++)
         {
             for ( int j=i+1;j<tabInfo.size();j++)
            {
                if (tabInfo.get(j).equals(tabInfo.get(i)))
                {
                tabInfo.remove(j);
                }
             }


         }

    }

   /* public void eliminateDuplicates(ArrayList<String> tabInfo)
    {

            for(String a  : tabInfo)
            {

                if(a.equals(tabInfo.get())) {
                    tabInfo.remove(a);
                }
            }

    }*/



}
