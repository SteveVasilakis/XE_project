# XE Challenge

**To Start Kafka consumer:**

*In terminal:*
1. pip install requirements.txt
2. python kafka_consumer.py '{host}' '{user}' '{pwd}' '{database}'

**SQL PART** 

*Classifieds Table:*


```
+--------------+--------------+------+-----+---------+-------+
| Field        | Type         | Null | Key | Default | Extra |
+--------------+--------------+------+-----+---------+-------+
| id           | varchar(50)  | NO   | PRI | NULL    |       |
| customer_id  | varchar(50)  | YES  |     | NULL    |       |
| created_at   | datetime     | YES  |     | NULL    |       |
| text         | varchar(200) | YES  |     | NULL    |       |
| ad_type      | text         | YES  |     | NULL    |       |
| price        | float        | YES  |     | NULL    |       |
| currency     | text         | YES  |     | NULL    |       |
| payment_type | text         | YES  |     | NULL    |       |
| payment_cost | float        | YES  |     | NULL    |       |
+--------------+--------------+------+-----+---------+-------+
```

*Margin_per_hour Table:*
```
+--------------+--------------+------+-----+---------+--------+
| Field        | Type          | Null | Key | Default | Extra |
+--------------+---------------+------+-----+---------+-------+
| ad_type      | text          | YES  |     | NULL    |       |
| payment_type | text          | YES  |     | NULL    |       |
| margin       | decimal(5,2)  | YES  |     | NULL    |       |
| date         | datetime      | YES  |     | NULL    |       |
+--------------+---------------+------+-----+---------+-------+
```
**To find Margin for a specific period of time:**

Copy and paste content of Margin_Specific.sql script in Mysql terminal and set variables from_date/to_date to the desired time interval.

**To Schedule the event:**

mysql> SET GLOBAL event_scheduler = ON;

Then copy and paste content of Margin_Scheduler.sql into mysql terminal.
