WITH session_based AS (
select 		s.session_id
          ,s.user_id
          ,s.trip_id
          ,s.session_start
          ,s.session_end
          ,s.page_clicks
          ,s.flight_discount
          ,s.flight_discount_amount
          ,s.hotel_discount
          ,s.hotel_discount_amount
          ,s.flight_booked
          ,s.hotel_booked
          ,s.cancellation
          ,EXTRACT( EPOCH FROM (s.session_end-s.session_start)) AS session_duration
          ,f.origin_airport
          ,f.destination
          ,f.destination_airport
          ,f.seats
          ,f.return_flight_booked
          ,f.departure_time
          ,f.return_time
          ,f.checked_bags
					,f.trip_airline
          ,f.destination_airport_lat
          ,f.destination_airport_lon
          ,f.base_fare_usd
          ,h.hotel_name
          ,CASE WHEN h.nights < 0 THEN 1 ELSE h.nights END AS nights
          ,h.rooms
          ,h.check_in_time
          ,h.check_out_time 
          ,h.hotel_per_room_usd AS hotel_price_per_room_night_usd
  				,u.home_airport_lat
  				,u.home_airport_lon
  from sessions s
  left join users u
  on s.user_id = u.user_id
  left join flights f
  on s.trip_id = f.trip_id
  left join hotels h
  on s.trip_id = h.trip_id
  WHERE s.user_id IN (SELECT user_id
                     FROM sessions
                     WHERE session_start > '2023-01-04'
                     GROUP BY user_id
                     HAVING COUNT(*) > 7)

)
, session_user_based AS( SELECT 	user_id
					,SUM(page_clicks)						AS num_clicks
          ,COUNT(distinct session_id)	AS num_sessions
          ,AVG(session_duration)			AS avg_session_duration
				FROM session_based
				GROUP BY user_id
),
trip_based as(
SELECT 	 user_id
  			,COUNT(trip_id) as n_trips
  			,SUM(CASE WHEN flight_booked AND return_flight_booked THEN 2
  						WHEN flight_booked THEN 1
  						ELSE 0
  						END) AS n_flights
  			,SUM(CASE WHEN (flight_booked AND flight_discount) THEN 1
  						ELSE 0
  						END) AS n_disc_flightbooked
    		,SUM(CASE WHEN (flight_booked AND flight_discount IS FALSE) THEN 1
  					  ELSE 0
  			  	  END) AS n_nodisc_flightbooked
    		,SUM(CASE WHEN (hotel_booked AND hotel_discount) THEN 1
  						ELSE 0
  						END) AS n_disc_hotelbooked
    		,SUM(CASE WHEN (hotel_booked AND hotel_discount IS FALSE) THEN 1
  					  ELSE 0
  			  	  END) AS n_nodisc_hotelbooked
  			,SUM((hotel_price_per_room_night_usd * nights*rooms) * (1 - COALESCE(hotel_discount_amount,0))) AS money_spent_hotel
  			,AVG(EXTRACT(DAY FROM departure_time - session_end)) AS avg_time_before_trip
  			,AVG(haversine_distance(home_airport_lat, home_airport_lon, destination_airport_lat, destination_airport_lon)) AS avg_km_flown
  			,SUM(CASE WHEN ((EXTRACT(MONTH FROM session_start) = 1) AND (flight_booked OR hotel_booked)) THEN 1
             ELSE 0
             END) AS n_bookedinJan
    		,SUM(CASE WHEN ((EXTRACT(MONTH FROM session_start) = 2) AND (flight_booked OR hotel_booked)) THEN 1
             ELSE 0
             END) AS n_bookedinFeb
    			,SUM(CASE WHEN ((EXTRACT(MONTH FROM session_start) = 3) AND (flight_booked OR hotel_booked)) THEN 1
             ELSE 0
             END) AS n_bookedinMar
    		,SUM(CASE WHEN ((EXTRACT(MONTH FROM session_start) = 4) AND (flight_booked OR hotel_booked)) THEN 1
             ELSE 0
             END) AS n_bookedinApr
    			,SUM(CASE WHEN ((EXTRACT(MONTH FROM session_start) = 5) AND (flight_booked OR hotel_booked)) THEN 1
             ELSE 0
             END) AS n_bookedinMay
    		,SUM(CASE WHEN ((EXTRACT(MONTH FROM session_start) = 6) AND (flight_booked OR hotel_booked)) THEN 1
             ELSE 0
             END) AS n_bookedinJun
    			,SUM(CASE WHEN ((EXTRACT(MONTH FROM session_start) = 7) AND (flight_booked OR hotel_booked)) THEN 1
             ELSE 0
             END) AS n_bookedinJul
    		,SUM(CASE WHEN ((EXTRACT(MONTH FROM session_start) = 8) AND (flight_booked OR hotel_booked)) THEN 1
             ELSE 0
             END) AS n_bookedinAug
    			,SUM(CASE WHEN ((EXTRACT(MONTH FROM session_start) = 9) AND (flight_booked OR hotel_booked)) THEN 1
             ELSE 0
             END) AS n_bookedinSep
    		,SUM(CASE WHEN ((EXTRACT(MONTH FROM session_start) = 10) AND (flight_booked OR hotel_booked)) THEN 1
             ELSE 0
             END) AS n_bookedinOct
    			,SUM(CASE WHEN ((EXTRACT(MONTH FROM session_start) = 11) AND (flight_booked OR hotel_booked)) THEN 1
             ELSE 0
             END) AS n_bookedinNov
    		,SUM(CASE WHEN ((EXTRACT(MONTH FROM session_start) = 12) AND (flight_booked OR hotel_booked)) THEN 1
             ELSE 0
             END) AS n_bookedinDec
    		,SUM(CASE WHEN (flight_booked AND hotel_booked AND cancellation = FALSE) THEN 1
             ELSE 0
             END) AS n_flightandHotelBooking
    		,SUM(CASE WHEN (flight_booked AND hotel_booked = FALSE AND cancellation = FALSE) THEN 1
             ELSE 0
             END) AS n_OnlyflightBooking
     		,SUM(CASE WHEN (flight_booked=FALSE AND hotel_booked AND cancellation = FALSE) THEN 1
             ELSE 0
             END) AS n_OnlyHotelBooking
  
	FROM session_based
  WHERE trip_id IS NOT NULL
  AND trip_id NOT IN (SELECT distinct trip_id
                     FROM session_based
                     WHERE cancellation --cancellation is True)
                     )
  GROUP BY user_id
),
main_q as (
SELECT 		sub.*
					,EXTRACT(YEAR FROM age(now(), u.birthdate)) AS age
          ,u.gender
          ,u.married
          ,u.has_children
          ,u.home_country
          ,u.home_city
          ,EXTRACT(YEAR FROM age(now(), u.sign_up_date)) as time_spent
          ,t.n_trips
          ,t.n_flights
  				,t.n_disc_flightbooked
          ,t.n_nodisc_flightbooked
  				,t.n_disc_hotelbooked
  				,t.n_nodisc_hotelbooked
          ,t.money_spent_hotel
          ,t.avg_km_flown
  				,t.n_bookedinJan
 					,t.n_bookedinFeb
   				,t.n_bookedinMar
 					,t.n_bookedinApr
    			,t.n_bookedinMay
 					,t.n_bookedinJun
   				,t.n_bookedinJul
 					,t.n_bookedinAug
  				,t.n_bookedinSep
 					,t.n_bookedinOct
    			,t.n_bookedinNov
 					,t.n_bookedinDec
  				,t.n_flightandHotelBooking
  				,t.n_OnlyflightBooking
  				,t.n_OnlyHotelBooking
FROM users u
LEFT JOIN session_user_based as sub
ON sub.user_id = u.user_id
JOIN trip_based t 
ON sub.user_id = t.user_id)
SELECT 	*
				, CASE 	WHEN age > 55 THEN 'senior traveller'
        				WHEN has_children THEN 'family travellers'
                WHEN age < 35 and n_trips < 2 THEN 'dreamer traveller'
                WHEN age < 35 and n_trips >=2 THEN 'young frequent traveller'
                WHEN age >=35 and n_trips > 5 THEN 'business traveller'
                ELSE 'others'
          END as groups
from main_q
LIMIT 30000;
