import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Calendar;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.map.MultiValueMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Scanner;
public class Main {



	public static void main(String[] args) throws SQLException, InterruptedException, IOException {
		
		
		
	    Scanner myObj = new Scanner(System.in);
	    System.out.println("Enter username for dbms");
	    String userName = myObj.nextLine();  
	    System.out.println("Username is: " + userName);
		
		
	    System.out.println("Enter password");
	    String password = myObj.nextLine();  
	    System.out.println("Password is: " + password);
	    
	    System.out.println("Enter name of schema for masterdata");
	    String masterdata = myObj.nextLine();  
	    System.out.println("Masterdata schema is: " + masterdata);
		
    	 Connection connection,dwh;

		 connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/"+masterdata, userName, password);
		 dwh= DriverManager.getConnection("jdbc:mysql://localhost:3306/dwh", userName, password);
		 
		 System.out.println("Safely Connected to DBMS");
		 
		 MeshJoin temp =new MeshJoin(connection,dwh,10,50);
		 temp.join();
		 

    }


}