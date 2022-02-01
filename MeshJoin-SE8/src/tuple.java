import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import java.sql.*;


public class tuple {
	
	private String product_id;
	private String customer_id;
	private String store_id;
	private String customer_name;
	private String store_name;
	private Date t_date;
	private int quantity;
	private Integer transaction_id;
	public int trans;
	public tuple()
	{
		this.product_id="";
		this.customer_id="";
		this.store_id="";
		this.customer_name="";
		this.store_name="";
		this.t_date=null;
		this.quantity=0;
		this.transaction_id=0;
		this.trans=this.transaction_id;
	}
	public tuple(ResultSet transactions) throws SQLException
	{
		
		this.product_id=transactions.getString("PRODUCT_ID");
		this.customer_id=transactions.getString("CUSTOMER_ID");
		this.store_id=transactions.getString("STORE_ID");
		this.customer_name=transactions.getString("CUSTOMER_NAME");
		this.store_name=transactions.getString("STORE_NAME");
		this.t_date=transactions.getDate("T_DATE");
		this.quantity=transactions.getInt("QUANTITY");
		this.transaction_id=transactions.getInt("TRANSACTION_ID");
		this.trans=this.transaction_id;
//		this.key=new joinattr(transactions.getInt("TRANSACTION_ID"),transactions.getString("PRODUCT_ID"));
		
	}
	
	public String getProductID()
	{
		return this.product_id;
	}

	public String getCustomerID()
	{
		return this.customer_id;
	}
	public String getCustomerName()
	{
		return this.customer_name;
	}
	public String getStoreID()
	{
		return this.store_id;
	}
	public String getStoreName()
	{
		return this.store_name;
	}
	public int getQuantity()
	{
		return this.quantity;
	}
	public Date getDate()
	{
		return this.t_date;
	}
	public int getID()
	{
		return this.transaction_id;
	}
	
	@Override
	public boolean equals(Object obj)
	{
		tuple x=(tuple) obj;
////		System.out.print("Transaction ID: "+this.transaction_id +" Object TD : "+x.transaction_id+"\n");
		if(this.transaction_id == x.transaction_id)
			return true;

		
		return false;
	}
	
	
	  @Override
	   public int hashCode() {
	     return transaction_id.hashCode();
	   }
	
	  public void display()
	  {
		  
//			System.out.print(transactions.getInt("TRANSACTION_ID")+"\n");
//			System.out.print(transactions.getString("PRODUCT_ID")+"\n");
//			System.out.print(transactions.getString("CUSTOMER_ID")+"\n");
//			System.out.print(transactions.getString("CUSTOMER_NAME")+"\n");
//			System.out.print(transactions.getString("STORE_ID")+"\n");
//			System.out.print(transactions.getString("STORE_NAME")+"\n");
//			System.out.print(transactions.getDate("T_DATE")+"\n");
//			System.out.print(transactions.getInt("QUANTITY")+"\n");
		  
//		  System.out.print("Product ID: "+getProductID()+" Transaction ID: "+getID()+ " Customer ID: "
//				  +getCustomerID()+" Customer Name: "+ getCustomerName() +" Store ID: "+getStoreID()
//				  +" Store Name: "+getStoreName()+" Quantity: "+getQuantity()+"\n");
	  }

}

