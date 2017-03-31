#Your sql follows

#Create table schools
create table schools (region string, district string, city string, id int, name string, level string, series int) row format delimited fields terminated by ',';

#Load data into schools
load data inpath '/home/andres/p2exam1/escuelasPR.csv' into table schools;

#Create table students
create table students (region string, district string, id int, name string, level string, sex string, sid int) row format delimited fields terminated by ',';

#Load data into students
load data inpath '/home/andres/p2exam1/studentsPR.csv' into table students;

#Part II

#1) Number of schools in each region and city
select region, city, count(*) from schools group by region, city;
#Number of schools in each region
select region, count(*) from schools group by region;

#2) Total number of students per school
select name, count(*) from students group by name;

#3) Find all the students that go to school in the city of Ponce or in Cabo Rojo.
select ST.* from students as ST, schools as SC where SC.id = ST.id and SC.city in ('Ponce', 'Cabo Rojo');

#4) Find the total number of students per region and city.
select SC.region, SC.city, count(*) from students as ST, schools as SC where SC.id = ST.id group by SC.region, SC.city;
#Number of students per region
select SC.region, count(*) from students as ST, schools as SC where SC.id = ST.id group by SC.region;