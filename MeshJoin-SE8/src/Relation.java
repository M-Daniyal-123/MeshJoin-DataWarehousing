
import java.sql.ResultSet;
import java.sql.SQLException;
public class Relation {
	private String product_id;
	private String product_name;
	private String supplier_id;
	private String supplier_name;
	private float price;
	
	public Relation(ResultSet obj) throws SQLException
	{
		this.product_id=obj.getString("PRODUCT_ID");
		this.product_name=obj.getString("PRODUCT_NAME");
		this.supplier_id=obj.getString("SUPPLIER_ID");
		this.supplier_name=obj.getString("SUPPLIER_NAME");
		this.price=obj.getFloat("PRICE");
	}
	public Relation()
	{
		
		this.product_id="";
		this.product_name="";
		this.supplier_id="";
		this.supplier_name="";
		this.price=0;
	}
	
	public float getPrice()
	{
		return this.price;
	}
	
	public void copy(ResultSet obj) throws SQLException
	{
		this.product_id=obj.getString("PRODUCT_ID");
		this.product_name=obj.getString("PRODUCT_NAME");
		this.supplier_id=obj.getString("SUPPLIER_ID");
		this.supplier_name=obj.getString("SUPPLIER_NAME");
		this.price=obj.getFloat("PRICE");
		
	}
	
	public String getProductID()
	{
		return this.product_id;
	}
	
	public String getSupplierID()
	{
		return this.supplier_id;
	}
	
	public String getSupplierName()
	{
		
		return this.supplier_name;
	}
	public String getProductName()
	{
		return this.product_name;
	}
	
	public void display()
	{
		
		System.out.print("Product ID: "+(this.getProductID())+" Product Name: "+(this.getProductName())
				 + " Supplier ID: "+(this.supplier_id)+ " Supplier Name: " + (this.supplier_name) + (" Price: ")
				+ (this.price) );
		
	}
	
}
