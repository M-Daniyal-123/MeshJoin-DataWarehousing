## Question 1
with question_1 as (
select sp.supplier_name as Supplier,t.qtr_ as quarter,t.month_ as MONTH,sum(sf.sales) as sales
from sales_fact sf
join supplier sp on (sp.supplier_id=sf.supplier_id)
join time t on (t.time_id = sf.time_id)
group by t.qtr_,sp.supplier_id,t.month_
order by t.date_) select supplier,
sum(
	case
		when q1.quarter = 1
        Then q1.sales 
        Else 0
	END

) as qtr_1,
sum(
	case
		when q1.quarter = 2
        Then q1.sales 
        Else 0
	END

) as qtr_2,
sum(
	case
		when q1.quarter = 3
        Then q1.sales 
        Else 0
	END

) as qtr_3,
sum(
	case
		when q1.quarter = 4
        Then q1.sales 
        Else 0
	END

) as qtr_4,
sum(
	case
		when q1.month = "January"
        Then q1.sales 
        Else 0
	END

) as Jan,
sum(
	case
		when q1.month = "Feb"
        Then q1.sales 
        Else 0
	END

) as Feb,
sum(
	case
		when q1.month = "March"
        Then q1.sales 
        Else 0
	END

) as March,
sum(
	case
		when q1.month = "April"
        Then q1.sales 
        Else 0
	END

) as April,
sum(
	case
		when q1.month = "May"
        Then q1.sales 
        Else 0
	END

) as May,
sum(
	case
		when q1.month = "June"
        Then q1.sales 
        Else 0
	END

) as June,
sum(
	case
		when q1.month = "July"
        Then q1.sales 
        Else 0
	END

) as July,
sum(
	case
		when q1.month = "Aug"
        Then q1.sales 
        Else 0
	END

) as Aug,
sum(
	case
		when q1.month = "Sept"
        Then q1.sales 
        Else 0
	END

) as Sept,
sum(
	case
		when q1.month = "Oct"
        Then q1.sales 
        Else 0
	END

) as Oct,
sum(
	case
		when q1.month = "Nov"
        Then q1.sales 
        Else 0
	END

) as Nov,
sum(
	case
		when q1.month = "Dec"
        Then q1.sales 
        Else 0
	END

) as Dec_
from question_1 q1 group by supplier;


; 

## Question 2
select st.store_name as Store,p.product_name as Product,sum(sf.sales) as sales
from sales_fact sf
join store st on (sf.store_id=st.store_id)
join products p on (p.product_id = sf.product_id)
group by p.product_id,st.store_id;




## Question 3
select p.product_name,sum(quantity) as total_quantity from sales_fact sf
join time t on (sf.time_id = t.time_id )
join products p on (p.product_id = sf.product_id)
where t.day = "Saturday" OR t.day ="Sunday"
group by p.product_name
order by total_quantity desc limit 5;




## Question 4
with non_pivot as
(
select p.product_name,t.qtr_ as quarter, sum(sf.sales) as sales 
from sales_fact sf
join products p on (p.product_id=sf.product_id)
join time t on (t.time_id = sf.time_id)
group by p.product_id,t.qtr_
) select p.product_name,sum(
	case
		when p.quarter = 1
        Then p.sales 
        Else Null
	END
) as qtr_1,sum(
	case
		when p.quarter = 2
        Then p.sales 
        Else Null
	END
) as qtr_2,sum(
	case
		when p.quarter = 3
        Then p.sales 
        Else Null
	END
) as qtr_3,sum(
	case
		when p.quarter = 4
        Then p.sales 
        Else Null
	END
) as qtr_4 from non_pivot p group by p.product_name;




### Question 5
with temp as
(
select p.product_name,t.qtr_ as quarter, sum(sf.sales) as sales 
from sales_fact sf
join products p on (p.product_id=sf.product_id)
join time t on (t.time_id = sf.time_id)
group by p.product_id,t.qtr_
) select p.product_name,sum(
	case
		when p.quarter = 1 Or p.quarter=2
        Then p.sales 
        Else Null
	END
) as first_half,sum(
	case
		when p.quarter = 3 or p.quarter =4
        Then p.sales 
        Else Null
	END
) as second_half,sum(
	case
		when p.quarter in (1,2,3,4)
        Then p.sales 
        Else Null
	END
) as total_yearly_sales from temp p group by p.product_name;


### Question 7
create view STOREANALYSIS_MV as
(
select store_id,product_id,sum(sales) as sales
from sales_fact group by store_id,product_id
);

select * from STOREANALYSIS_MV;

#### Question 6
### For anomaly use masterdata schema
### and run the following query

#select * from masterdata where product_name = "Tomatoes";




select sum(sales) from sales_fact;





