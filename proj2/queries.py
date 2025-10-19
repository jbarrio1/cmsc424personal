queries = ["" for i in range(0, 4)]

queries[0] = """
select 0;
"""

### 1.
queries[1] = ["count(customerid)","airports left outer join (flewon_cust7 natural join flights) on airportid = source"]
### <answer1>
queries[1][0] = "count(customerid)"
### <answer2>
queries[1][1] = "airports left outer join (flewon_cust7 natural join flights) on airportid = source"


### 2.
queries[2] = ["select city", "group by airportid order by airportid"]
### <answer1>
queries[2][0] = "select city"
### <answer2>
queries[2][1] = "group by airportid,city order by airportid"

### 3.
### Explaination - The issues arises from the where clause not filtering out a customer enteriely if they have a non null value after the join. That is
## if a customer has 6 flights, 1 out of jfk, the where clause will remove/not keep that entry and the customer will have 5 flights, then the group by
# executes, then the having does, and that customer that should've have been removed entirely has <=5 flights passing the having clause  
###
queries[3] = """
select cid 
from customer_flights c left join flights_JFK j 
  on c.flightid = j.flightid 
where not exists (select * from customer_flights c2 left join flights_JFK j2 on c2.flightid = j2.flightid where c.cid = c2.cid and j2.flightid is not null)
group by cid 
having count(*) <= 5;
"""