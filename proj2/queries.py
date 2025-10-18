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
### Explaination - 
###
queries[3] = """
select cid 
from customer_flights c left join flights_JFK j 
  on c.flightid = j.flightid 
where j.flightid is null 
group by cid 
having count(*) <= 5;
"""