# -*- coding: utf-8 -*-
"""
@author: RMJcar
"""
from pulp import *
import pandas as pd
import timeit
#parameters
discount=.8
vot=15
circuityfactor=1.2 # ratio of true distance/ Euclidean distance
starttime='2018-09-05 00:00' # in the format of 'YYYY-MM-DD HH24:MI'
endtime='2018-09-06 23:59' # in the format of 'YYYY-MM-DD HH24:MI'
simple=False #Make False if you want all columns on the output
start_query=timeit.default_timer()
from sqlalchemy import create_engine
passwordfile = open ......
password=passwordfile.read()
passwordfile.close()
engine = create_engine(.......

pair_query="""
/* select all the CTrain stations */
with station as (
select station_name,
       longitude,
       latitude
from rmj.calgary_stations),

/* find all FHV trips that begin in a quarter mile square centered at each station*/
trips as (
select tnc.tripnum as  trip_id,
	   tnc.tripstart as pickup_datetime,
	   tnc.trip_start_longitude as pickup_longitude, 
	   tnc.trip_start_latitude as pickup_latitude, 
	   tnc.trip_end_longitude as dropoff_longitude, 
       tnc.trip_end_latitude as dropoff_latitude, 
	   {circuity_factor}*(point(tnc.trip_start_longitude, tnc.trip_start_latitude) <@> 
                          point(tnc.trip_end_longitude,	tnc.trip_end_latitude) ) as trip_distance,
       station.station_name,
       point(tnc.trip_start_longitude, tnc.trip_start_latitude) <@> 
                          point(station.longitude,	station.latitude) as station_distance 
from rmj.calgary_tnc tnc
cross join station
where tnc.tripstart between to_timestamp('{start}','YYYY-MM-DD HH24:MI') and 
                              to_timestamp('{end}','YYYY-MM-DD HH24:MI') and
                    /*within eighth of a mile from the station */
      (point(tnc.trip_start_longitude, tnc.trip_start_latitude) <@> 
                          point(station.longitude,	station.latitude) )<0.125),
/*Since CTrain stations downtown are very closer together, find the closest
  CTrain station */
mintrip as (
      select trip_id,
             min(station_distance) as min_distance
      from trips
      group by trip_id),
/* Estimate fare per Calgary policy */
finaltrip as (
      select trips.trip_id,
             trips.station_name,
             trips.pickup_datetime,
             trips.pickup_longitude,
             trips.pickup_latitude,
             trips.dropoff_longitude,
             trips.dropoff_latitude,
             trips.trip_distance,
             case when trips.trip_distance<.0745 then 3.80 
             else 3.80 + ((trips.trip_distance/0.0745)-.0745)*.2 end as fare_amount
from trips inner join mintrip
on trips.trip_id = mintrip.trip_id and
   trips.station_distance = mintrip.min_distance)									   
/* create pairs of trips with 5 minutes of each other and under capacity */
select orig.trip_id as first_trip_id,
	   pair.trip_id as second_trip_id,
       orig.station_name,
       orig.trip_distance	as recalc_orig_first_distance,
       pair.trip_distance	as recalc_orig_second_distance,
	   orig.fare_amount + pair.fare_amount as combined_fare,
       orig.fare_amount as first_fare,
       pair.fare_amount as second_fare,
	   {circuity_factor}*(point(orig.dropoff_longitude,	orig.dropoff_latitude) <@> point(pair.dropoff_longitude,	pair.dropoff_latitude))	as distance_to_second,
	   orig.trip_id || '_' || pair.trip_id as pair_id,
       orig.pickup_longitude as first_pickup_longitude, 
	   orig.pickup_latitude as first_pickup_latitude, 
       pair.pickup_longitude as second_pickup_longitude,
	   pair.pickup_latitude as second_pickup_latitude,
       orig.dropoff_longitude as first_dropoff_longitude, 
	   orig.dropoff_latitude as first_dropoff_latitude, 
       pair.dropoff_longitude as second_dropoff_longitude,
	   pair.dropoff_latitude as second_dropoff_latitude,
       orig.pickup_datetime as first__pickup_datetime,
       pair.pickup_datetime as second__pickup_datetime
from finaltrip  orig left join finaltrip  pair
on orig.trip_id <> pair.trip_id and
   orig.station_name = pair.station_name and
   orig.trip_distance < pair.trip_distance and -- drop off closest first
   orig.pickup_datetime - pair.pickup_datetime between '-00:05' and  '00:05' 				 					 
"""
pair_query=pair_query.format(circuity_factor = circuityfactor,start = starttime,end=endtime)
pairs_data = pd.read_sql_query(pair_query, engine)
stop_query=timeit.default_timer()
print('Query time: ' + str((stop_query-start_query)/60.0) + " mintutes")
pairs_data['filter']=pairs_data.apply(lambda x: pd.notnull(x['distance_to_second']),axis=1)
unique_trips=pairs_data['first_trip_id'].unique().tolist()
data=pairs_data[pairs_data['filter']==True].copy() #only include pairs
#define optimization model
m = pulp.LpProblem('TSP', pulp.LpMaximize)
#create list of unique trip_ids
first=data['first_trip_id'].unique().tolist()
second=data['second_trip_id'].unique().tolist()
combo=list(set(first+second))
pairs = {}
for i in range(len(data)):
    # create decision variable for each pair
    pairs[i]=pulp.LpVariable(data.iloc[i]['pair_id'],0,1, pulp.LpBinary)
    # add to objective (maximize fare and minimize excess distance)
m += pulp.lpSum(pairs[i]*(discount*data.iloc[i]['combined_fare']-vot*data.iloc[i]['distance_to_second']) for i in range(len(data)) )
#create constraint (each trip can be selected at most once)
for i in combo:
    tours=list()
    for j in range(len(pairs)):
        if str(i) in str(pairs[j]):
            tours.append(pairs[j])
    m += pulp.lpSum(tours) <= 1
m.solve() #solve
stop_optimize=timeit.default_timer()
print('Optimize time: ' + str((stop_optimize-stop_query)/60.0) + " mintutes")
#extract variables into dictionary
varsdict = {}
for v in m.variables():
    varsdict[v.name] = v.varValue

#output
#add whether tour was chosen
pairs_data['selected_'+str(discount)+'_'+str(vot)]=pairs_data.apply(lambda x:varsdict[x['pair_id']] if pd.notnull(x['pair_id']) else None, axis=1)
pairs_data['Benchmark $/mile']=pairs_data.apply(lambda x: x['combined_fare']/(x['recalc_orig_first_distance']+x['recalc_orig_second_distance']) if x['selected_'+str(discount)+'_'+str(vot)]==1 else None, axis=1)
pairs_data['Alternative operator $/mile']=pairs_data.apply(lambda x: (discount*x['combined_fare'])/(x['recalc_orig_first_distance']+x['distance_to_second']) if x['selected_'+str(discount)+'_'+str(vot)]==1 else None, axis=1)
pairs_data['Alternative User $/mile']=pairs_data.apply(lambda x: (discount*x['combined_fare'])/(x['recalc_orig_first_distance']+x['recalc_orig_second_distance']) if x['selected_'+str(discount)+'_'+str(vot)]==1 else None, axis=1)
pairs_data['Additional Miles %']=pairs_data.apply(lambda x: (x['recalc_orig_first_distance']+x['distance_to_second'])/(x['recalc_orig_second_distance']) if x['selected_'+str(discount)+'_'+str(vot)]==1 else None, axis=1)
#some stats
totalnumtours=len(pairs_data[pairs_data['selected_'+str(discount)+'_'+str(vot)]==1])
totalfare=pairs_data[pairs_data['selected_'+str(discount)+'_'+str(vot)]==1]['combined_fare'].sum()
totalorigdistance=pairs_data[pairs_data['selected_'+str(discount)+'_'+str(vot)]==1]['recalc_orig_first_distance'].sum()+pairs_data[pairs_data['selected_'+str(discount)+'_'+str(vot)]==1]['recalc_orig_second_distance'].sum()
totalnewdistance=pairs_data[pairs_data['selected_'+str(discount)+'_'+str(vot)]==1]['recalc_orig_first_distance'].sum()+pairs_data[pairs_data['selected_'+str(discount)+'_'+str(vot)]==1]['distance_to_second'].sum()
totaldistancetosecond=pairs_data[pairs_data['selected_'+str(discount)+'_'+str(vot)]==1]['distance_to_second'].sum()
totaloriginalseconddistance=pairs_data[pairs_data['selected_'+str(discount)+'_'+str(vot)]==1]['recalc_orig_second_distance'].sum()
print('Total number of taxi trips: ' + str(len(unique_trips)))
print('Total number of trips that were paired: ' + str(2*len(pairs_data[pairs_data['selected_'+str(discount)+'_'+str(vot)]==1])))
print('Average excess distance: ' + str(pairs_data[pairs_data['selected_'+str(discount)+'_'+str(vot)]==1]['distance_to_second'].mean()))
print('Average Benchmark $/mile: ' + str(totalfare/totalorigdistance))
print('Average Alternative operator $/mile: ' + str((discount*totalfare)/totalnewdistance))
print('Average Alternative User $/mile: ' + str((discount*totalfare)/totalorigdistance))
print('Average Additional Miles %' + str(totalnewdistance/totaloriginalseconddistance))
if simple == True:
    pairs_data=pairs_data[['pair_id','recalc_orig_first_distance','recalc_orig_second_distance','distance_to_second','combined_fare','selected_'+str(discount)+'_'+str(vot),'Benchmark $/mile','Alternative operator $/mile',	'Alternative User $/mile','Additional Miles %']]
#Change the below to where you want the model results to be exported
pairs_data.to_csv('C:/Users/RMJca/Documents/calgary/taxi_out_3.csv')
