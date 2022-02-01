import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Calendar;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

import java.io.IOException;
import java.sql.*;
public class MeshJoin {
	private Connection conn;
	private Connection dwh;
	private Relation_d masterdata;
	private Stream transactions;
	private ArrayBlockingQueue<List<tuple>> stream_queue;
	private int num_of_relations; //same for queue size
	private MultiValuedMap<String,tuple> map = new ArrayListValuedHashMap<>();
	private int stream_size;
	private int off;
	private int counter=0;
	public MeshJoin(Connection conn,Connection dwh,int num_of_iterations, int stream_size)
	{
		this.conn=conn;
		this.stream_size=stream_size;
		this.num_of_relations=num_of_iterations;
		this.masterdata=new Relation_d(this.num_of_relations,conn);
		this.transactions=new Stream(this.stream_size,conn);
		this.stream_queue=new ArrayBlockingQueue<List<tuple>>(num_of_iterations);
		this.off=1;
		this.dwh=dwh;
		
	}
	public void ProcessStream() throws SQLException
	{
		List<tuple> snew=this.transactions.next();
//		ResultSet stream=this.transactions.next();
		
		
		if(this.stream_queue.size()==this.num_of_relations || snew==null)
		{
			
			List<tuple> sfinal=stream_queue.poll();
			
			for(int i=0;i<sfinal.size();i++)
			{
				tuple temp=sfinal.get(i);
				map.removeMapping(temp.getProductID(), temp);
			}		
			
		}
		
		if (snew == null) {
			if(stream_queue.isEmpty())
				this.off=0;
			return;
		}
		
		for(int i=0;i<snew.size();i++)
		{
			tuple temp=snew.get(i);
			map.put(temp.getProductID(), temp);
		}
		
		
		this.stream_queue.add(snew);
		

		
//		stream.close();
	}
	private  String day(Calendar calendar)
	{
		if(calendar.get(calendar.DAY_OF_WEEK) ==  calendar.MONDAY)
			return "Monday";
		if(calendar.get(calendar.DAY_OF_WEEK) ==  calendar.TUESDAY)
			return "Tuesday";
		if(calendar.get(calendar.DAY_OF_WEEK) ==  calendar.WEDNESDAY)
			return "Wednesday";
		if(calendar.get(calendar.DAY_OF_WEEK) ==  calendar.THURSDAY)
			return "Thursday";
		if(calendar.get(calendar.DAY_OF_WEEK) ==  calendar.FRIDAY)
			return "Friday";
		if(calendar.get(calendar.DAY_OF_WEEK) ==  calendar.SATURDAY)
			return "Saturday";
		if(calendar.get(calendar.DAY_OF_WEEK) ==  calendar.SUNDAY)
			return "Sunday";
		
		return null;
		
				
	}
	
	
	public int process_time(java.util.Date date ) throws SQLException
	{
		
		String months[]={"January","Feb","March","April","May","June","July","Aug","Sept","Oct","Nov","Dec"};
		Date temp=(Date) date;
		try {
			PreparedStatement dwh_query=dwh.prepareStatement("select * from time where date_ = ?");
			dwh_query.setDate(1, temp);
			ResultSet time_table=dwh_query.executeQuery();
			time_table.next();
			int time_d=time_table.getInt("time_id");
			
			return time_d;
			
		}catch (Exception e)
		{
			
			 int qtr=1;
			 Calendar calendar = Calendar.getInstance();
	
			 calendar.setTime(temp);
			 int month_num=calendar.get(calendar.MONTH)+1;
			 if(month_num>9)
				 qtr=4;
			 else if(month_num>6)
				 qtr=3;
			 else if (month_num>3)
				 qtr=2;
		     else if (month_num>1)
		    	 qtr=1;
			 
			 
			 PreparedStatement dwh_query=dwh.prepareStatement("insert into time(date_,day,week_,month_,qtr_,year_) values (?,?,?,?,?,?)");
			 dwh_query.setDate(1, temp);
			 dwh_query.setString(2, day(calendar));
			 dwh_query.setInt(3, calendar.get(calendar.WEEK_OF_MONTH));
			 dwh_query.setString(4, months[calendar.get(calendar.MONTH)]);
			 dwh_query.setInt(5, qtr);
			 dwh_query.setInt(6, calendar.get(calendar.YEAR));
			 dwh_query.executeUpdate();
			
		}
		
		PreparedStatement dwh_query=dwh.prepareStatement("select * from time where date_ = ?");
		dwh_query.setDate(1, temp);
		ResultSet time_table=dwh_query.executeQuery();
		time_table.next();
		int time_id=time_table.getInt("time_id");
		
		
		return time_id;
		
		
		
	}
	
	public void process_products(String product_id,String product_name) throws SQLException
	{
		
		try {
			PreparedStatement dwh_query=dwh.prepareStatement("select * from products where product_id = ?");
			dwh_query.setString(1, product_id);
			ResultSet product_table=dwh_query.executeQuery();
			product_table.next();
			product_table.getString("PRODUCT_NAME");
			
		}catch (Exception e)
		{
			 PreparedStatement dwh_query=dwh.prepareStatement("insert into products(product_id,product_name) values (?,?)");
			 dwh_query.setString(1, product_id);
			 dwh_query.setString(2,product_name);
			 dwh_query.executeUpdate();

		} 
	}
	public void process_suppliers(String supplier_id,String supplier_name) throws SQLException
	{
		try {
			PreparedStatement dwh_query=dwh.prepareStatement("select * from supplier where supplier_id = ?");
			dwh_query.setString(1, supplier_id);
			ResultSet supplier_table=dwh_query.executeQuery();
			supplier_table.next();
			supplier_table.getString("SUPPLIER_NAME");
			
		}catch (Exception e)
		{
			 
			 PreparedStatement dwh_query=dwh.prepareStatement("insert into supplier(supplier_id,supplier_name) values (?,?)");
			 dwh_query.setString(1, supplier_id);
			 dwh_query.setString(2,supplier_name);
			 
			 dwh_query.executeUpdate();

		} 

	}
	
	public void process_store(String store_id,String store_name) throws SQLException
	{
		
		try {
			PreparedStatement dwh_query=dwh.prepareStatement("select * from store where store_id = ?");
			dwh_query.setString(1, store_id);
			ResultSet store_table=dwh_query.executeQuery();
			store_table.next();
			store_table.getString("STORE_NAME");
			
		}catch (Exception e)
		{
			 
			 PreparedStatement dwh_query=dwh.prepareStatement("insert into store(store_id,store_name) values (?,?)");
			 dwh_query.setString(1, store_id);
			 dwh_query.setString(2,store_name);
			 
			 dwh_query.executeUpdate();

			
		} 

	}
	public void process_customer(String customer_id,String customer_name) throws SQLException
	{
		
		try {
			PreparedStatement dwh_query=dwh.prepareStatement("select * from customers where customer_id = ?");
			dwh_query.setString(1, customer_id);
			ResultSet customer_table=dwh_query.executeQuery();
			customer_table.next();
			customer_table.getString("CUSTOMER_NAME");
			
		}catch (Exception e)
		{
			 
			 PreparedStatement dwh_query=dwh.prepareStatement("insert into customers(customer_id,customer_name) values (?,?)");
			 dwh_query.setString(1, customer_id);
			 dwh_query.setString(2,customer_name);
			 
			 dwh_query.executeUpdate();

		} 

	}
	
	
	
	public void output(tuple tra,ResultSet md) throws SQLException
	{
		
		String customer_id=tra.getCustomerID();
		String product_id=md.getString("PRODUCT_ID");
		String supplier_id=md.getString("SUPPLIER_ID");
		int time_id=process_time(tra.getDate());
		String store_id=tra.getStoreID();
		
		
		process_store(tra.getStoreID(),tra.getStoreName());
		process_products(md.getString("PRODUCT_ID"),md.getString("PRODUCT_NAME"));
		process_suppliers(md.getString("SUPPLIER_ID"),md.getString("SUPPLIER_NAME"));
		process_customer(tra.getCustomerID(),tra.getCustomerName());
		
		
		try {
			
			PreparedStatement dwh_query=dwh.prepareStatement("select * from sales_fact where customer_id = ? and supplier_id = ?"
					+ " and product_id = ? and store_id = ? and time_id = ?");
			dwh_query.setString(1, customer_id);
			dwh_query.setString(2, supplier_id);
			dwh_query.setString(3,product_id);
			dwh_query.setString(4,store_id);
			dwh_query.setInt(5,time_id);
			
			
			ResultSet fact_table=dwh_query.executeQuery();
			fact_table.next();
			int quantity=fact_table.getInt("QUANTITY");
			float sales=fact_table.getFloat("sales");
			
		
			
			quantity =(quantity +(tra.getQuantity()));
			sales=(sales+(md.getFloat("PRICE")*tra.getQuantity()));
			
			PreparedStatement update=dwh.prepareStatement("UPDATE sales_fact SET quantity =?,sales=? where customer_id = ? and supplier_id = ?"
					+ " and product_id = ? and store_id = ? and time_id = ?");
			
			
			update.setInt(1, quantity);
			update.setFloat(2, sales);
			update.setString(3, customer_id);
			update.setString(4, supplier_id);
			update.setString(5,product_id);
			
			update.setString(6,store_id);
			update.setInt(7,time_id);
			update.executeUpdate();

			
		}catch(Exception e)
		{
		
			float sales=tra.getQuantity()*md.getFloat("PRICE");
			int quanity=tra.getQuantity();
			
			PreparedStatement dwh_query=dwh.prepareStatement("Insert into sales_fact(customer_id,supplier_id,product_id,store_id,time_id,sales,quantity) values(?,?,?,?,?,?,?)");
			dwh_query.setString(1, customer_id);
			dwh_query.setString(2, supplier_id);
			dwh_query.setString(3,product_id);
			dwh_query.setString(4,store_id);
			dwh_query.setInt(5,time_id);
			dwh_query.setFloat(6, sales);
			dwh_query.setInt(7, quanity);
			
			dwh_query.executeUpdate();
		}
		
	}
	
	
	public void join() throws SQLException
	{
		int counter=0;
		while(true)
		{
			ProcessStream();
			
			if(this.off == 0)
			{
				System.out.println("Stream Ended");
				System.out.println(counter);
				return;
				
			}
			ResultSet relation=masterdata.next();
			
			while(relation.next())
			{
				Collection<tuple> matched=map.get(relation.getString("PRODUCT_ID"));
				for (tuple s:matched)
				{
					output(s,relation);
					counter++;
				}
				
			}
			relation.close();
			
		}
		
		
		
		
	}
	

}
