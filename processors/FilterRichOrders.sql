-- ** Continuous Filter ** 
-- Performs a continuous filter based on a WHERE condition.
--          .----------.   .----------.   .----------.              
--          |  SOURCE  |   |  INSERT  |   |  DESTIN. |              
-- Source-->|  STREAM  |-->| & SELECT |-->|  STREAM  |-->Destination
--          |          |   |  (PUMP)  |   |          |              
--          '----------'   '----------'   '----------'               
-- STREAM (in-application): a continuously updated entity that you can SELECT from and INSERT into like a TABLE
-- PUMP: an entity used to continuously 'SELECT ... FROM' a source STREAM, and INSERT SQL results into an output STREAM

-- Create output stream, which can be used to send to a destination
CREATE OR REPLACE STREAM "DESTINATION_USER_STREAM" ("items" VARCHAR(128), "order_id" VARCHAR(64), "total_cost" DECIMAL(5,2), "user_id" VARCHAR(8), "email" VARCHAR(32), "first_name" VARCHAR(8), "last_name" VARCHAR(16));

-- Create pump to insert into output 
CREATE OR REPLACE PUMP "STREAM_PUMP" AS INSERT INTO "DESTINATION_USER_STREAM"
    SELECT STREAM "items", "order_id", "total_cost", "user_id", "email", "first_name", "last_name"
        FROM "SOURCE_SQL_STREAM_001"
        WHERE "total_cost" >= 100;
