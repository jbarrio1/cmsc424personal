-- creating the function for the trigger
-- customers(custid, name,bday, ff)
-- newcustomer(custid, name,bday)
-- ffairlines(custid,airlineid i.e ff, points, status )
CREATE OR REPLACE FUNCTION customers_updated() RETURNS trigger as $$ 
DECLARE 
points int;
stat varchar(6);
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO newcustomers(customerid, name, birthdate) VALUES (NEW.customerid,NEW.name,NEW.birthdate);
        IF NEW.frequentflieron is not null THEN
        -- calculate points 
        points = (with test as (select * from flights natural join (select flightid from flewon where customerid = NEW.customerid and flightid like NEW.frequentflieron || '%') 
         as newcust ) select sum (extract(hour from (local_arrival_time - local_departing_time))* 60 + extract(minute from (local_arrival_time- local_departing_time)) ) from test);
         --INSERT INTO newcustomers(customerid, name, birthdate) VALUES (NEW.customerid,NEW.name,NEW.birthdate);
         IF points >= 500 THEN
         stat = 'GOLD';
         ELSIF points < 750 and points >=500 THEN
         stat = 'SILVER';
         ELSE 
         IF points is null THEN points = 0; END IF;
         stat = 'BRONZE';
         END IF;
         INSERT INTO ffairlines(customerid,airlineid, points,status) VALUES (NEW.customerid, NEW.frequentflieron, points,stat);
         END IF;
         --INSERT INTO newcustomers(customerid, name, birthdate) VALUES (NEW.customerid,NEW.name,NEW.birthdate);
        END IF;
    
    IF TG_OP = 'UPDATE' THEN 
     UPDATE newcustomers SET customerid = NEW.customerid, name = NEW.name, birthdate = NEW.birthdate where customerid = NEW.customerid;
     IF NEW.frequentflieron is null THEN 
        DELETE FROM ffairlines where customerid = NEW.customerid; 
     END IF;
     IF NEW.frequentflieron not in (select airlineid from ffairlines where customerid = NEW.customerid) and not null THEN 
     points =  (with test as (select * from flights natural join (select flightid from flewon where customerid = NEW.customerid and flightid like NEW.frequentflieron || '%') 
         as newcust ) select sum (extract(hour from (local_arrival_time - local_departing_time))* 60 + extract(minute from (local_arrival_time- local_departing_time)) ) from test); 
        --INSERT INTO ffairlines(customerid,airlineid, points) VALUES (NEW.customerid, NEW.frequentflieron, points);
        IF points >= 500 THEN
         stat = 'GOLD';
         ELSIF points < 750 and points >=500 THEN
         stat = 'SILVER';
         ELSE 
         IF points is null THEN points = 0; END IF;
         stat = 'BRONZE';
         END IF;
        INSERT INTO ffairlines(customerid,airlineid, points,status) VALUES (NEW.customerid, NEW.frequentflieron, points,stat);
        END IF;
     END IF;

    IF TG_OP = 'DELETE' THEN
         DELETE FROM ffairlines where customerid = OLD.customerid; 
         DELETE FROM newcustomers where customerid = OLD.customerid;
        END IF;

   RETURN NULL;
END; 
$$ LANGUAGE plpgsql;
 
CREATE OR REPLACE TRIGGER customers_added AFTER INSERT OR UPDATE OR DELETE on customers
 FOR EACH ROW WHEN (pg_trigger_depth() = 0) EXECUTE PROCEDURE customers_updated();

-- Q2 
CREATE OR REPLACE FUNCTION newcustomers_update()  RETURNS trigger as $$
DECLARE
ff varchar(2);
BEGIN 
    IF TG_OP = 'INSERT' THEN
    ff = (select airlineid from ffairlines where customerid = NEW.customerid order by points desc, airlineid limit 1); 
    INSERT INTO customers(customerid, name, birthdate, frequentflieron) VALUES (NEW.customerid, NEW.name, NEW.birthdate,ff);
    END IF; 
     
    IF TG_OP =  'DELETE' THEN 
        DELETE FROM customers where customerid = OLD.customerid; 
    END IF; 

    IF TG_OP = 'UPDATE' THEN 
    UPDATE customers SET customerid = NEW.customerid, name = NEW.name, birthdate = NEW.birthdate WHERE customerid = NEW.customerid; 

    END IF; 


    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER newcustomers_interact AFTER INSERT OR UPDATE OR DELETE on newcustomers
FOR EACH ROW
WHEN (pg_trigger_depth()   = 0) 
EXECUTE PROCEDURE newcustomers_update();

-- Q3

CREATE OR REPLACE FUNCTION ffairlines_update() RETURNS trigger as $$ 
DECLARE
max_airline varchar(2);
BEGIN
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
    -- find out after,update,or delete what airline now has the highpoints to assign to ff in customers 
    max_airline = (with max as (select max(points) as max_points from ffairlines where customerid = NEW.customerid group by customerid)
    select airlineid from ffairlines,max where points = max_points LIMIT 1);
        UPDATE customers SET frequentflieron = max_airline where customerid = NEW.customerid;
    END IF; 

    IF TG_OP = 'DELETE' THEN 
    -- find the next highest or null 
    max_airline = (with max as (select max(points) as max_points from ffairlines where customerid = OLD.customerid group by customerid)
    select airlineid from ffairlines,max where points = max_points LIMIT 1);
    UPDATE customers SET frequentflieron = max_airline where customerid = OLD.customerid;
    END IF;

    RETURN NULL;
END; 
$$ LANGUAGE plpgsql;


CREATE OR REPLACE TRIGGER ffairlines_updated AFTER INSERT OR UPDATE OR DELETE on ffairlines
FOR EACH ROW 
WHEN (pg_trigger_depth() <3) 
EXECUTE PROCEDURE ffairlines_update();