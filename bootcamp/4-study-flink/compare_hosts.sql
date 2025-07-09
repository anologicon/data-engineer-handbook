select 
	host,
	avg(num_hits) as num_hits,
	max(num_hits) as max_hits,
	sum(num_hits) as total_hits
	
from processed_events_aggregated_ip_host

where host in ('zachwilson.techcreator.io','zachwilson.tech','lulu.techcreator.io')

group by host