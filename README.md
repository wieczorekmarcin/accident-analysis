# Analysis of accident data using Apache Spark

## General description of the project
The goal of the project is the practical use of the Spark platform for data processing in class systems
Big Data.

## Subject
The data contains information on traffic incidents reported by users of various types of applications
navigational and more. In addition to data on the reported incident itself, its location, etc. include
they also provide weather information.

![diagram](/../master/src/main/resources/diagram.png?raw=true "diagram")

## Data origin
The project is implemented only on the basis of data provided by the teacher. The following information
they only explain the origin of the original data.
The data comes from the site
https://www.kaggle.com/sobhanmoosavi/us-accidents

## Data characteristics
Data on reported road incidents were divided into 4 sets simulating 4 independent databases
data with a similar pattern. 

### Time zone
The division was made based on the time zone in which the incidents occurred

* US/Central
* US/Mountain
* US/Eastern
* US/Pacific

Each set consists of two CSV files (simulating tables) containing:

### Geographical data
geo – geographical data:
* Zipcode,
* City,
* County,
* State,
* Country,
* Timezone

### Main data
mainData - data on reported incidents:
* ID - This is a unique identifier of the accident record
* Source - Indicates source of the accident report (i.e. the API which reported the accident.)
* TMC - A traffic accident may have a Traffic Message Channel (TMC) code which provides more
detailed description of the event.
* Severity- Shows the severity of the accident, a number between 1 and 4, where 1 indicates the least
impact on traffic (i.e., short delay as a result of the accident) and 4 indicates a significant impact on
traffic (i.e., long delay).
* Start_Time - Shows start time of the accident in local time zone.
* End_Time - Shows end time of the accident in local time zone.
* Start_Lat - Shows latitude in GPS coordinate of the start point.
* Start_Lng - Shows longitude in GPS coordinate of the start point.
* End_Lat - Shows latitude in GPS coordinate of the end point.
* End_Lng - Shows longitude in GPS coordinate of the end point.
* Distance(mi) - The length of the road extent affected by the accident.
* Description - Shows natural language description of the accident.
* Number - Shows the street number in address field.
* Street - Shows the street name in address field.
* Side - Shows the relative side of the street (Right/Left) in address field.
* Zipcode - Shows the zipcode in address field.
* Airport_Code - Denotes an airport-based weather station which is the closest one to location of the accident.
* Amenity - A POI annotation which indicates presence of amenity in a nearby location.
* Bump - A POI annotation which indicates presence of speed bump or hump in a nearby location.
* Crossing - A POI annotation which indicates presence of crossing in a nearby location.
* Give_Way - A POI annotation which indicates presence of give_way in a nearby location.
* Junction - A POI annotation which indicates presence of junction in a nearby location.
* No_Exit - A POI annotation which indicates presence of no_exit in a nearby location.
* Railway - A POI annotation which indicates presence of railway in a nearby location.
* Roundabout - A POI annotation which indicates presence of roundabout in a nearby location.
* Station - A POI annotation which indicates presence of station in a nearby location.
* Stop - A POI annotation which indicates presence of stop in a nearby location.
* Traffic_Calming - A POI annotation which indicates presence of traffic_calming in a nearby location.
* Traffic_Signal - A POI annotation which indicates presence of traffic_signal in a nearby location.
* Turning_Loop - A POI annotation which indicates presence of turning_loop in a nearby location.
* Sunrise_Sunset - Shows the period of day (i.e. day or night) based on sunrise/sunset.
* Civil_Twilight - Shows the period of day (i.e. day or night) based on civil twilight.
* Nautical_Twilight - Shows the period of day (i.e. day or night) based on nautical twilight.
* Astronomical_Twilight - Shows the period of day (i.e. day or night) based on astronomical twilight.

### Weather data
Weather data is available in the text file weather.txt. They contain information about the date and
measurement time, weather description with detailed values of individual measures (temperature, humidity
etc.) and the place (Airport_Code) where the weather was recorded.

## Results
Ten rows from each result set.

### Geography
```
+----------+-----------+----------+-----+-------+----------+------------+
|Zipcode   |City       |County    |State|Country|Timezone  |geography_id|
+----------+-----------+----------+-----+-------+----------+------------+
|32405-7085|Panama City|Bay       |FL   |US     |US/Central|0           |
|32505-5520|Pensacola  |Escambia  |FL   |US     |US/Central|1           |
|32536-7239|Crestview  |Okaloosa  |FL   |US     |US/Central|2           |
|32539-7125|Crestview  |Okaloosa  |FL   |US     |US/Central|3           |
|32583-9460|Milton     |Santa Rosa|FL   |US     |US/Central|4           |
|35004     |Moody      |St. Clair |AL   |US     |US/Central|5           |
|35051-3761|Columbiana |Shelby    |AL   |US     |US/Central|6           |
|35080-3637|Helena     |Shelby    |AL   |US     |US/Central|7           |
|35080-7516|Helena     |Shelby    |AL   |US     |US/Central|8           |
|35094-3789|Leeds      |Jefferson |AL   |US     |US/Central|9           |
|35207-3179|Birmingham |Jefferson |AL   |US     |US/Central|10          |
```

### Accidents
```
+---------+--------+-----+--------+-------------------+--------------------+-------------------------------------------------------------------------------------------------------+----------+------------+-----------+
|ID       |Source  |TMC  |Severity|Start_Time         |Distance            |Description                                                                                            |Zipcode   |Airport_Code|weather_id |
+---------+--------+-----+--------+-------------------+--------------------+-------------------------------------------------------------------------------------------------------+----------+------------+-----------+
|A-1003114|MapQuest|201.0|2       |2018-07-05 10:53:00|0.0                 |Accident on Nottingham Oaks Trl at Memorial Dr.                                                        |77079-6201|KSGR        |42949773311|
|A-1020812|MapQuest|201.0|2       |2018-07-16 10:53:00|0.0                 |Accident on Bauman Rd at Canino Rd.                                                                    |77076-1205|KIAH        |42949787693|
|A-1049101|MapQuest|201.0|2       |2018-05-31 15:53:00|0.0                 |Accident on LA-73 Jefferson Hwy at LA-426 Old Hammond Hwy.                                             |70809-1180|KBTR        |42949724205|
|A-1087425|MapQuest|201.0|2       |2018-06-20 11:36:00|0.0                 |Accident on State St at Highland Rd.                                                                   |70802-7918|KBTR        |42949753943|
|A-1113366|MapQuest|201.0|2       |2018-05-02 17:56:00|0.0                 |Accident on Linwood Ave near 63rd St.                                                                  |71106-2602|KSHV        |42949678436|
|A-1145712|MapQuest|201.0|2       |2018-05-17 18:35:00|0.0                 |Accident on Tremont St at Snowden St.                                                                  |77028-1425|KMCJ        |42949702805|
|A-1259856|MapQuest|201.0|3       |2018-03-14 09:53:00|0.0                 |Accident on I-24 Westbound after Exit 56 TN-255 Harding Pl.                                            |37211     |KBNA        |34359848875|
|A-1268851|MapQuest|201.0|2       |2018-03-19 07:51:00|0.0                 |Accident on Babcock Rd at Hausman Rd.                                                                  |78249     |KSAT        |34359855828|
|A-1375308|MapQuest|201.0|3       |2018-01-11 12:48:00|0.0                 |Accident on I-35W Southbound at Exits 11 12 12B Diamond Lake Rd.                                       |55419     |KMSP        |34359752476|
|A-1375774|MapQuest|229.0|2       |2018-01-11 21:11:00|0.0                 |Slow traffic due to accident on US-61 Airline Hwy at Delcourt Ave.                                     |70815     |KBTR        |34359753478|
```



### Date
```
+----------+                                                                    
|date      |
+----------+
|2019-04-01|
|2019-03-31|
|2019-03-30|
|2019-03-29|
|2019-03-28|
|2019-03-27|
|2019-03-26|
|2019-03-25|
|2019-03-24|
|2019-03-23|
```

### Weather
```
+---------------------+--------------------+---------------------+-------------------------+----------------------+-----------------------+-------------------------+---------------------------------+------------+-------------------+----------+
|wind_chill           |humidity            |pressure             |visibility               |wind_direction        |wind_speed             |precipitation            |weather_condition                |airport_code|date               |weather_id|
+---------------------+--------------------+---------------------+-------------------------+----------------------+-----------------------+-------------------------+---------------------------------+------------+-------------------+----------+
| Wind Chill (F): ~   | Humidity (%): 100.0| Pressure (in): 29.65| Visibility (miles): 10.0| Wind Direction: Calm | Wind Speed (mph): ~   | Precipitation (in): 0.0 | Weather Condition: Light Rain   |KCMH        |2016-02-08 05:51:00|0         |
| Wind Chill (F): ~   | Humidity (%): 97.0 | Pressure (in): 29.7 | Visibility (miles): 10.0| Wind Direction: Calm | Wind Speed (mph): ~   | Precipitation (in): 0.02| Weather Condition: Overcast     |KLUK        |2016-02-08 05:53:00|1         |
| Wind Chill (F): ~   | Humidity (%): 91.0 | Pressure (in): 29.68| Visibility (miles): 10.0| Wind Direction: Calm | Wind Speed (mph): ~   | Precipitation (in): 0.02| Weather Condition: Light Rain   |KFFO        |2016-02-08 05:58:00|2         |
| Wind Chill (F): ~   | Humidity (%): 55.0 | Pressure (in): 29.65| Visibility (miles): 10.0| Wind Direction: Calm | Wind Speed (mph): ~   | Precipitation (in): ~   | Weather Condition: Overcast     |KAKR        |2016-02-08 06:54:00|3         |
| Wind Chill (F): 33.3| Humidity (%): 100.0| Pressure (in): 29.67| Visibility (miles): 10.0| Wind Direction: SW   | Wind Speed (mph): 3.5 | Precipitation (in): ~   | Weather Condition: Overcast     |KI69        |2016-02-08 06:56:00|4         |
| Wind Chill (F): 31.0| Humidity (%): 96.0 | Pressure (in): 29.64| Visibility (miles): 9.0 | Wind Direction: SW   | Wind Speed (mph): 4.6 | Precipitation (in): ~   | Weather Condition: Mostly Cloudy|KDAY        |2016-02-08 07:38:00|5         |
| Wind Chill (F): 30.7| Humidity (%): 93.0 | Pressure (in): 29.64| Visibility (miles): 5.0 | Wind Direction: WNW  | Wind Speed (mph): 5.8 | Precipitation (in): ~   | Weather Condition: Rain         |KTZR        |2016-02-08 07:50:00|6         |
| Wind Chill (F): 35.5| Humidity (%): 97.0 | Pressure (in): 29.63| Visibility (miles): 7.0 | Wind Direction: SSW  | Wind Speed (mph): 3.5 | Precipitation (in): 0.03| Weather Condition: Light Rain   |KCMH        |2016-02-08 07:51:00|7         |
| Wind Chill (F): 33.3| Humidity (%): 89.0 | Pressure (in): 29.65| Visibility (miles): 6.0 | Wind Direction: SW   | Wind Speed (mph): 3.5 | Precipitation (in): ~   | Weather Condition: Mostly Cloudy|KMGY        |2016-02-08 07:53:00|8         |
| Wind Chill (F): 29.8| Humidity (%): 93.0 | Pressure (in): 29.69| Visibility (miles): 10.0| Wind Direction: WSW  | Wind Speed (mph): 10.4| Precipitation (in): 0.01| Weather Condition: Light Rain   |KLUK        |2016-02-08 07:53:00|9         |
| Wind Chill (F): 31.0| Humidity (%): 100.0| Pressure (in): 29.66| Visibility (miles): 7.0 | Wind Direction: WSW  | Wind Speed (mph): 3.5 | Precipitation (in): ~   | Weather Condition: Overcast     |KDAY        |2016-02-08 07:56:00|10        |
```

### Final result
```
|date      |geography_id|sum_accidents|avg_accidents_severity|avg_accidents_distance|
+----------+------------+-------------+----------------------+----------------------+
|2018-06-21|25143       |18           |2.888888888888889     |0.42394444470933323   |
|2018-12-03|5606        |18           |3.3333333333333335    |0.28388888650444444   |
|2018-09-21|51393       |17           |4.0                   |0.623                 |
|2018-01-17|19607       |16           |2.5                   |0.17043750089406248   |
|2017-05-15|63174       |16           |2.0                   |0.23693750000000002   |
|2016-12-16|25143       |16           |2.5625                |0.3321249999999999    |
|2017-09-11|49904       |14           |2.2857142857142856    |0.0                   |
|2016-12-06|25143       |14           |2.5                   |0.2314285714285714    |
|2016-11-28|25143       |14           |2.4285714285714284    |0.20971428571428566   |
|2018-04-09|39902       |14           |2.357142857142857     |0.7754285714285716    |
```