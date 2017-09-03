package hsfulda.de;

import fjdbc.FedConnection;
import fjdbc.FedException;
import fjdbc.FedPseudoDriver;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main(String[] args) throws FedException {

        String usernameTest = "VDBSA05";
        String passwordTest = "VDBSA05";

        String usernameValidation = "VDBSA05";
        String passwordValidation = "VDBSA05";

        FedConnection fedConnection;

		/*
		 * Test schema
		 */
       try {
           fedConnection = (new FedPseudoDriver()).getConnection(usernameValidation, passwordValidation);

           FedTestEnvironment fedTestEvironment = new FedTestEnvironment(fedConnection);

           fedTestEvironment.run("Test/DRPTABS.SQL", false);

           fedTestEvironment.run("Test/CREPARTABS.SQL", false);
           fedTestEvironment.run("Test/INSERTAIRPORTS.SQL", false);
           fedTestEvironment.run("Test/INSERTAIRLINES.SQL", false);
           fedTestEvironment.run("Test/INSERTPASSENGERS.SQL", false);
           fedTestEvironment.run("Test/INSERTFLIGHTS.SQL", false);
           fedTestEvironment.run("Test/INSERTBOOKINGS.SQL", false);
           fedTestEvironment.run("Test/PARSELCNTSTAR.SQL", true);

           fedTestEvironment.run("Test/PARSELS1T.SQL", true);
           fedTestEvironment.run("Test/PARSELS1OR.SQL", true);
           fedTestEvironment.run("Test/PARSELSJOIN1.SQL", true);
           fedTestEvironment.run("Test/PARSELS1TGP.SQL", true);
           fedTestEvironment.run("Test/PARSELS1TWGP.SQL", true);   //OPTIONAL
           fedTestEvironment.run("Test/PARSELS1TGHAV.SQL", true);  //OPTIONAL
           fedTestEvironment.run("Test/PARUPDS.SQL", true);
           fedTestEvironment.run("Test/PARINSERTS.SQL", true);
           fedTestEvironment.run("Test/PARDELS.SQL", true);
           fedTestEvironment.run("Test/PARSELCNTSTAR.SQL", true);

       } catch (FedException fedException) {
            fedException.printStackTrace();
        
       }



    }
}
