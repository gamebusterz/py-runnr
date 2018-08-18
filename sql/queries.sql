CREATE temp TABLE modp_uuids AS (
	SELECT user_id AS source_user_id, uuid_gen_udf(user_id,'user') AS user_id, module,uuid_gen_udf(module,'module') AS module_id 
	FROM module_progress_staging);

CREATE temp TABLE IF NOT EXISTS new_data_module_progress AS
(SELECT user_cohort_id,cohort_entity_id FROM
	(
		SELECT
		uc.user_cohort_id,
		ce.cohort_entity_id,
		ce.available_from,
		ce.deadline,
		mp.completed,
		mp.percent_complete,
		mp.score
		FROM (select * from module_progress_staging WHERE module_progress_staging.modified > COALESCE((SELECT MAX(timestamp_start) FROM audit WHERE data_source = 'module_progress' AND status = '{AUDIT_SUCCESS_FLAG}'),'2000-01-01 00:00:00')) mp
		JOIN modp_uuids modpu
			ON mp.user_id = modpu.source_user_id
			AND mp.module = modpu.module
		LEFT JOIN (select distinct cohort_id,module_id from dw.cohort_entity_mapping WHERE current = TRUE) cem
			ON modpu.module_id = cem.module_id
		LEFT JOIN (select * from dw.cohort_entity WHERE type = 'module' AND current = TRUE) ce
			ON modpu.module_id = ce.cohort_entity_id
		JOIN (select * from dw.user_cohort WHERE current = TRUE) uc
			ON modpu.user_id = uc.user_id
			AND cem.cohort_id = uc.cohort_id
		EXCEPT ALL
		SELECT
		user_cohort_id,
		cohort_entity_id,
		start_date,
		end_date,
		completed,
		percent_complete,
		score
		FROM dw.user_cohort_progress WHERE current = TRUE) sbq);

UPDATE dw.user_cohort_progress 
SET to_time = module_progress_staging.modified, current = FALSE, audit_id = 43
FROM 
	module_progress_staging 
	JOIN modp_uuids
		ON module_progress_staging.user_id = modp_uuids.source_user_id
		AND module_progress_staging.module = modp_uuids.module 
	LEFT JOIN (SELECT DISTINCT cohort_id,module_id FROM dw.cohort_entity_mapping WHERE current = TRUE) cem
		ON modp_uuids.module_id = cem.module_id
	LEFT JOIN (SELECT * FROM dw.cohort_entity WHERE type = 'module' AND current = TRUE) ce
		ON module_progress_staging.module = ce.source_entity_id 
	JOIN (SELECT * FROM dw.user_cohort WHERE current = TRUE) uc 
		ON uc.cohort_id = cem.cohort_id
WHERE 
dw.user_cohort_progress.user_cohort_id = uc.user_cohort_id
AND dw.user_cohort_progress.cohort_entity_id = ce.cohort_entity_id 
AND dw.user_cohort_progress.current = TRUE
AND NOT EXISTS (SELECT user_cohort_id FROM new_data_module_progress WHERE dw.user_cohort_progress.user_cohort_id = user_cohort_id)
AND NOT EXISTS (SELECT cohort_entity_id FROM new_data_module_progress WHERE dw.user_cohort_progress.cohort_entity_id = cohort_entity_id);