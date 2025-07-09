select 
	avg(num_hits) avg_web_events_user_session
	
from processed_events_aggregated_ip_host

where host like '%techcreator%'