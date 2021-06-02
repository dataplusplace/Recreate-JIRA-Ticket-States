WITH RECURSIVE tickets_cte AS	-- Pull ticket current state
(		
SELECT 	ji.id AS issueid
		,pj.pname AS project
		,pj.pkey
		,ji.issuenum
		,pj.pkey || '-' || ji.issuenum AS issue_key
		,it.pname AS issue_type
		,ji.assignee AS curr_assignee
		,ji.created AT TIME ZONE 'America/New_York' AS created_ts
		,iss.pname AS curr_status
		,r.pname AS resolution
		,ji.resolutiondate AT TIME ZONE 'America/New_York' AS resolved_ts
FROM	jira_iss ji
        INNER JOIN iss_type it
            ON ji.issuetype = it.id
        INNER JOIN iss_status iss
            ON ji.issuestatus = iss.id
        LEFT JOIN res r
            ON ji.resolution = r.id
        LEFT JOIN proj pj
            ON ji.project = pj.id
WHERE	pj.pkey IN ('proj1','proj2','proj3','proj4')	
AND		it.pname IN ('type1','type2','type3','type4')	
AND		((ji.resolutiondate AT TIME ZONE 'America/New_York')::DATE >= '2020-01-01'
		OR ji.resolutiondate IS NULL	
		)	
ORDER BY pj.pkey		
		,ji.issuenum	
)
, status_cte AS -- Pull status history
(		
SELECT	tk.issue_key
		,cg.created AT TIME ZONE 'America/New_York' AS status_ts
		,ci.oldstring AS prev_status
		,ci.newstring AS new_status
FROM 	tickets_cte tk
		INNER JOIN change_grp cg
			ON cg.issueid = tk.issueid
		INNER JOIN change_itm ci
			ON cg.id = ci.groupid
			AND ci.field = 'status'
)
, status_rank_cte AS -- Join status history with current state data and order by change timestamp
(		
SELECT 	tk.issue_key
		,tk.issue_type
		,tk.created_ts
		,ch.status_ts
		,ch.prev_status
		,ch.new_status
		,tk.curr_status
		,tk.resolution
		,tk.resolved_ts
		,RANK() OVER(PARTITION BY tk.issue_key ORDER BY ch.status_ts) AS rnk
FROM 	tickets_cte tk
		LEFT JOIN status_cte ch
			ON tk.issue_key = ch.issue_key
)
, full_status_changes_cte AS -- Recreate missing original state status to create full list of all status ticket states
(
--Add status from original state where status was set at ticket creation and later changed
SELECT 	issue_key	
		,created_ts	
		,created_ts AS status_ts
		,NULL AS prev_status
		,prev_status AS new_status
		,resolved_ts
FROM 	status_rank_cte
WHERE 	rnk = 1	
AND		prev_status IS NOT NULL
UNION ALL		
--Add status from current state where working status was set at ticket creation and has not changed
SELECT 	ar.issue_key
		,ar.created_ts
		,ar.created_ts AS status_ts
		,NULL AS prev_status
		,tk.curr_status AS new_status
		,tk.resolved_ts
FROM 	status_rank_cte ar
		LEFT JOIN tickets_cte tk
			ON ar.issue_key = tk.issue_key
WHERE 	ar.rnk = 1
AND		ar.status_ts IS NULL
UNION ALL		
--Include remaining records except those where status was set at ticket creation and has not changed
SELECT 	issue_key
		,created_ts
		,status_ts
		,prev_status
		,new_status
		,resolved_ts
FROM 	status_rank_cte
WHERE 	NOT
		(rnk = 1
		AND status_ts IS NULL
		)
ORDER BY issue_key
		,status_ts
)
, status_rerank_cte AS -- Order by timestamp where status changed from a non-working status code to a working code or vice versa, change status to catch-all working term
(
SELECT	issue_key
		,created_ts	
		,status_ts	
		,CASE
			WHEN COALESCE(prev_status,'') IN ('status1','status2','status3','status4')
			THEN 'Working'
			ELSE 'Non-working'
			END AS prev_status
		,CASE
			WHEN COALESCE(new_status,'') IN ('status1','status2','status3','status4')
			THEN 'Working'
			ELSE 'Non-working'
			END AS new_status
		,resolved_ts
		,RANK() OVER(PARTITION BY issue_key ORDER BY status_ts) AS rnk
FROM	full_status_changes_cte
WHERE	(COALESCE(prev_status,'') IN ('status1','status2','status3','status4')
		AND COALESCE(new_status,'') NOT IN ('status1','status2','status3','status4')
		)
OR		(COALESCE(new_status,'') IN ('status1','status2','status3','status4')
		AND COALESCE(prev_status,'') NOT IN ('status1','status2','status3','status4')
		)
)
, final_status_stage_cte AS -- Pivot to create list of status start and end time at the ticket level
(
SELECT	new_status AS status
		,issue_key
		,status_ts AS status_start
		,COALESCE(
			LEAD(status_ts,1)
			OVER(PARTITION BY issue_key ORDER BY rnk
			)
			,resolved_ts
			,DATE_TRUNC('day', NOW() AT TIME ZONE 'America/New_York')
		) AS status_end
FROM 	status_rerank_cte
)
, final_status_cte AS
(
SELECT	status -- Exclude date ranges that fall ouside 2020 and non-working status states
		,issue_key
		,status_start
		,status_end
FROM	final_status_stage_cte
WHERE	status = 'Working'
AND		(status_start::DATE, status_end::DATE) OVERLAPS ('2020-01-01'::DATE, '2020-12-31'::DATE)
)
, assignee_cte AS -- Pull assignee history
(		
SELECT	tk.issue_key
		,cg.created AT TIME ZONE 'America/New_York' AS assigned_ts
		,ci.oldvalue AS prev_assignee
		,ci.newvalue AS new_assignee
FROM 	tickets_cte tk	
		INNER JOIN change_grp cg	
			ON cg.issueid = tk.issueid
		INNER JOIN change_itm ci	
			ON cg.id = ci.groupid
			AND ci.field = 'assignee'
)		
, assignee_rank_cte AS -- Join assignee history with current state data and rank by assigned timestamp
(		
SELECT 	tk.issue_key
		,tk.issue_type
		,tk.created_ts
		,ch.assigned_ts
		,ch.prev_assignee
		,ch.new_assignee
		,tk.curr_status
		,tk.resolution
		,tk.resolved_ts
		,RANK() OVER(PARTITION BY tk.issue_key ORDER BY ch.assigned_ts)	AS rnk
FROM 	tickets_cte tk
		LEFT JOIN assignee_cte ch
			ON tk.issue_key = ch.issue_key
)
, full_assignee_changes_cte AS -- Recreate missing original state assignees to create full list of all assignee ticket states
(
--Add assignee from original state where assignee was set at ticket creation and later reassigned
SELECT 	issue_key	
		,created_ts	
		,created_ts AS assigned_ts	
		,NULL AS prev_assignee	
		,prev_assignee AS new_assignee
		,resolved_ts
FROM 	assignee_rank_cte
WHERE 	rnk = 1	
AND		prev_assignee IS NOT NULL
UNION ALL		
--Add assignee from current state where assignee was set at ticket creation and not reassigned
SELECT 	ar.issue_key	
		,ar.created_ts
		,ar.created_ts AS assigned_ts	
		,NULL AS prev_assignee	
		,tk.curr_assignee AS new_assignee
		,tk.resolved_ts
FROM 	assignee_rank_cte ar
		LEFT JOIN tickets_cte tk	
			ON ar.issue_key = tk.issue_key
WHERE 	ar.rnk = 1	
AND		ar.assigned_ts IS NULL	
UNION ALL		
--Include remaining records except those where assignee was set at ticket creation and not reassigned
SELECT 	issue_key
		,created_ts
		,assigned_ts
		,prev_assignee
		,new_assignee
		,resolved_ts
FROM 	assignee_rank_cte
WHERE 	NOT
		(rnk = 1
		AND assigned_ts IS NULL
		)
ORDER BY issue_key
		,assigned_ts
)
, assignee_rerank_cte AS -- Rank by timestamp and exclude tickets with no assignee
(
SELECT	issue_key
		,created_ts	
		,assigned_ts	
		,prev_assignee	
		,new_assignee
		,resolved_ts
		,RANK() OVER(PARTITION BY issue_key ORDER BY assigned_ts) AS rnk
FROM	full_assignee_changes_cte
WHERE	prev_assignee IS NOT NULL
OR		new_assignee IS NOT NULL
)
, final_assignee_stage_cte AS -- Pivot to create list of start and end time at the assignee level
(
SELECT	new_assignee AS assignee
		,issue_key
		,assigned_ts AS assigned_start
		,COALESCE(
			LEAD(assigned_ts,1)
				OVER(PARTITION BY issue_key ORDER BY rnk
			)
			,resolved_ts
			,DATE_TRUNC('day', NOW() AT TIME ZONE 'America/New_York')
		) AS assigned_end
FROM 	assignee_rerank_cte
)
, final_assignee_cte AS
(
SELECT	assignee -- Exclude date ranges that fall ouside 2020 and cases where assignee was set blank
        ,issue_key
        ,assigned_start
        ,assigned_end
FROM	final_assignee_stage_cte
WHERE	assignee IS NOT NULL
AND		(assigned_start::DATE, assigned_end::DATE) OVERLAPS ('2020-01-01'::DATE, '2020-12-31'::DATE)
)
,final_assignee_ticket_cte AS -- Combine assignee and working status states for final ticket level data
(
SELECT	tk.project
		,tk.issue_key
-- 		,tk.issue_type
		,fa.assignee
		,CASE
			WHEN GREATEST(fa.assigned_start, st.status_start)::DATE < '2020-01-01'
			THEN DATE_TRUNC('day', '2020-01-01'::DATE AT TIME ZONE 'UTC')
			ELSE GREATEST(fa.assigned_start, st.status_start)
			END AS working_assigned_start
		,LEAST(fa.assigned_end, st.status_end) AS working_assigned_end
FROM	tickets_cte tk
		INNER JOIN final_assignee_cte fa
			ON tk.issue_key = fa.issue_key
		INNER JOIN final_status_cte st
			ON fa.issue_key = st.issue_key
			AND (fa.assigned_start, fa.assigned_end) OVERLAPS (st.status_start, st.status_end)
ORDER BY tk.pkey
		,tk.issuenum
)		
,merge_int_cte AS -- Recursively merge intervals at project and assignee level for min/max timestamps
(
SELECT	project
		,assignee
		,working_assigned_start
		,working_assigned_end
FROM	final_assignee_ticket_cte
UNION 
SELECT 	fat.project
		,fat.assignee
	 	,least(mi.working_assigned_start, fat.working_assigned_start)
	 	,greatest(mi.working_assigned_end, fat.working_assigned_end)
FROM 	merge_int_cte mi
		INNER JOIN final_assignee_ticket_cte fat
			ON mi.project = fat.project
			AND mi.assignee = fat.assignee
			AND (fat.working_assigned_start, fat.working_assigned_end) OVERLAPS (mi.working_assigned_start, mi.working_assigned_end)
)
,project_assignee_cte AS -- Project level assignee start/end times
(
SELECT 	project
		,assignee
	 	,MIN(working_assigned_start) AS date_start
	 	,MAX(working_assigned_end) AS date_end
FROM 	merge_int_cte
GROUP BY 1,2
ORDER BY 2,3
)
,project_count_cte AS -- Determine number of projects assignee was active in daily
(
SELECT	pa.assignee
		,dt.days
		,COUNT(1) AS pjs
FROM	project_assignee_cte pa
		LEFT JOIN (SELECT (GENERATE_SERIES('2020-01-01', CURRENT_DATE - 1, '1 day'::INTERVAL))::DATE AS days) dt
			ON dt.days BETWEEN pa.date_start::DATE AND pa.date_end::DATE
GROUP BY 1,2
)
SELECT	pa.assignee
		,pa.project
		,dt.days
		,CASE
			WHEN pa.date_start::DATE = dt.days
			AND pa.date_end::DATE != dt.days
			THEN (EXTRACT(EPOCH FROM (dt.days::TIMESTAMP) + INTERVAL '1 day' - INTERVAL '1 second')
				- EXTRACT(EPOCH FROM (pa.date_start))
			) / 3600
			WHEN pa.date_end::DATE = dt.days
			AND pa.date_start::DATE != dt.days
			THEN (EXTRACT(EPOCH FROM (pa.date_end))
				- EXTRACT(EPOCH FROM (dt.days::TIMESTAMP))
			) / 3600
			WHEN pa.date_start::DATE = dt.days
			AND pa.date_end::DATE = dt.days
			THEN (EXTRACT(EPOCH FROM (pa.date_end))
				- EXTRACT(EPOCH FROM (pa.date_start))
			) / 3600
			ELSE 24.0
			END 
		/ pc.pjs AS hours
FROM	project_assignee_cte pa
		INNER JOIN (SELECT (GENERATE_SERIES('2020-01-01', CURRENT_DATE - 1, '1 day'::INTERVAL))::DATE AS days) dt
			ON dt.days BETWEEN pa.date_start::DATE AND pa.date_end::DATE
		LEFT JOIN project_count_cte pc
			ON pa.assignee = pc.assignee
			AND dt.days = pc.days
ORDER BY pa.project
		,dt.days
		,pa.assignee
;

			   