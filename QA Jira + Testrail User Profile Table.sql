-- Databricks notebook source

-- COMMAND ----------

CREATE WIDGET TEXT dateToProcess  DEFAULT 'null';

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW game_teams_clean AS
SELECT
 UPPER(TRIM(TRANSLATE(testrail_user_name, 'ÁáÉéÍíÓóÚú', 'AaEeIiOoUu'))) AS testrail_user_name
 , CASE WHEN main_jira_project = '' THEN null ELSE main_jira_project END AS main_jira_project
 , CASE WHEN start_dt = '' THEN null ELSE start_dt END AS start_dt
 , CASE WHEN role_name = '' THEN null ELSE role_name END AS role_name
 , active_ind
FROM game_teams_raw
;

CREATE OR REPLACE TABLE testrail_users_main_jira_projects 
USING delta 
LOCATION 'TABLE_LOCATION'
AS
SELECT * FROM game_teams_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Filter QA Projects

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vo_projects_lkp AS
SELECT * 
FROM pr_lookup_schema.jira_projects
WHERE project_name IN ('Project1','Project2','Project3','Project4','Project5','Project6')
OR (project_name LIKE ('QC%') AND project_name NOT IN ('Project7','Project8','Project9','Project10','Project11'))
;

SELECT * FROM vo_projects_lkp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Users and Main Project
-- MAGIC
-- MAGIC Just those with both Jira and Testrail data 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vo_testrail_users AS
SELECT DISTINCT 
    TRANSLATE(assigned_to_name, 'ÁáÉéÍíÓóÚú', 'AaEeIiOoUu') AS assigned_to_name 
  , assigned_to_mail
FROM dv_schema.testrail_data
WHERE UPPER(status_id) <> 'UNTESTED'
;

CREATE OR REPLACE TEMP VIEW vo_testrail_users_main_jira_projects AS
SELECT 
 testrail_user_name
 , assigned_to_mail
 , main_jira_project
 , start_dt
 , role_name
FROM dv_lookup_schema.testrail_users_main_jira_projects a
LEFT JOIN vo_testrail_users b
ON a.testrail_user_name = UPPER(TRIM(b.assigned_to_name))
WHERE main_jira_project IS NOT NULL
  AND active_ind = 1
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Sprints
-- MAGIC
-- MAGIC * If 2 or more sprints have the same start_dt, group them together
-- MAGIC * Use complete_dt instead of end_dt. If 2 or more sprints have the same start_dt and distinct complete_dt, we will take into account the longest time frame

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vo_sprints AS
SELECT
  *
  , DATEDIFF(next_sprint_start_dt, max_complete_dt) AS diff
  , DATEDIFF(next_sprint_start_dt, start_dt) AS start_diff
FROM
(
SELECT
  *
  , LEAD(start_dt) OVER(PARTITION BY project_name_long ORDER BY start_dt ASC) AS next_sprint_start_dt
  , LEAD(sprints_started) OVER(PARTITION BY project_name_long ORDER BY start_dt ASC) AS next_sprints_started
  , LEAD(max_end_dt) OVER(PARTITION BY project_name_long ORDER BY start_dt ASC) AS next_sprint_end_dt
  , LEAD(max_complete_dt) OVER(PARTITION BY project_name_long ORDER BY start_dt ASC) AS next_sprint_complete_dt
FROM
(
SELECT
  project_name AS project_name_long -- Here is a CASE statement, which has been removed for data protection reasons.
  , start_dt
  , current_state_name
  , COLLECT_LIST(DISTINCT sprint_name) AS sprints_started
  , MAX(end_dt) AS max_end_dt
  , MAX(complete_dt) AS max_complete_dt
FROM 
  pr_schema.jira_sprints sp
INNER JOIN
  (SELECT DISTINCT project_name, board_id FROM vo_projects_lkp) lkp
ON sp.project_name = lkp.project_name
  AND sp.project_id = lkp.board_id
WHERE UPPER(sprint_name) NOT LIKE "%AUTOMATION%"
  AND start_dt IS NOT NULL
GROUP BY 1,2,3
)
WHERE project_name_long IS NOT NULL
)
;

-- COMMAND ----------

-- Sprints that need to be combined (too close start_dt)
CREATE OR REPLACE TEMP VIEW vo_sprints_combined_aux AS
SELECT
  project_name_long
  , start_dt
  , current_state_name
  , ARRAY_UNION(sprints_started, next_sprints_started) AS sprints_started
  , CASE
      WHEN max_end_dt < next_sprint_end_dt THEN next_sprint_end_dt
      ELSE max_end_dt
    END AS max_end_dt
  , CASE
      WHEN max_complete_dt < next_sprint_complete_dt THEN next_sprint_complete_dt
      ELSE max_complete_dt
    END AS max_complete_dt
FROM vo_sprints
WHERE start_diff < 7

;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vo_unique_sprints AS
SELECT
  *
  , CASE
      WHEN next_sprint_start_dt < max_complete_dt THEN
        CASE
          WHEN next_sprint_start_dt < max_end_dt THEN next_sprint_start_dt
          WHEN next_sprint_start_dt = max_end_dt THEN DATE_SUB(max_end_dt,1)
          ELSE max_end_dt
        END
      WHEN next_sprint_start_dt = max_complete_dt THEN DATE_SUB(max_complete_dt,1)
      ELSE max_complete_dt
    END AS final_date
FROM
(
  SELECT
    *
    , LEAD(start_dt) OVER(PARTITION BY project_name_long ORDER BY start_dt ASC) AS next_sprint_start_dt
  FROM
  (
    -- Sprints that don't need to be combined
    SELECT
      project_name_long
      , start_dt
      , current_state_name
      , sprints_started
      , max_end_dt
      , max_complete_dt
    FROM vo_sprints sp
    LEFT ANTI JOIN 
    (
    SELECT
      project_name_long
      , EXPLODE(sprints_started) AS sprints_to_exclude 
    FROM
      vo_sprints_combined_aux
    ) ex
    ON sp.project_name_long = ex.project_name_long
      AND ARRAY_CONTAINS(sp.sprints_started, ex.sprints_to_exclude)
    WHERE start_diff >= 7

    UNION ALL
    -- Sprints that need to be combined (too close start_dt)
    SELECT * FROM vo_sprints_combined_aux
  )
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Jira Issues

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vo_date AS
SELECT MAX(process_dt) AS max_dt FROM pr_schema.jira_issues
WHERE process_dt >= CURRENT_DATE - 21

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vo_issues AS
SELECT 
  /*broadcast(dates)*/
  created_at_dt
  , project_name
  , issue_id
  , issue_name
  , parent_issue_name
  , issue_type_name
  , sprint_hist_lst
  , points_qty
  , priority_cd
  , status_name
  , CAST(status_hist_lst[0].modified_at AS DATE) AS done_dt -- The last status is allways in position 0
  , status_hist_lst
  , UPPER(TRIM(TRANSLATE(assignee_name, 'ÁáÉéÍíÓóÚú', 'AaEeIiOoUu'))) AS assignee_name
  , CASE WHEN names.testrail_user_name IS NOT NULL THEN 1 ELSE 0 END AS assignee_on_testrail
  , reporter_name
  , game_name
FROM pr_schema.jira_issues jira
LEFT JOIN vo_testrail_users_main_jira_projects names
  ON UPPER(TRIM(TRANSLATE(assignee_name, 'ÁáÉéÍíÓóÚú', 'AaEeIiOoUu'))) = names.testrail_user_name
INNER JOIN 
  (SELECT max_dt FROM vo_date) dates
ON jira.process_dt = dates.max_dt  
WHERE status_name = 'Done'
  AND CAST(status_hist_lst[0].modified_at AS DATE) >= '2020-12-01'
; 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vo_issues_parents_lst AS
SELECT
    issue_name AS parent_issue_name
  , issue_type_name
  , points_qty AS parent_points_qty
  , total_issues
FROM vo_issues i 
INNER JOIN 
(
SELECT parent_issue_name, COUNT(*) AS total_issues FROM vo_issues WHERE parent_issue_name IS NOT NULL GROUP BY 1
)p 
ON i.issue_name = p.parent_issue_name
;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vo_unit_issues AS
SELECT
    game_name_long -- Here is a CASE statement, which has been removed for data protection reasons.
  , issue_id
  , i.issue_name
  , i.issue_type_name
  , i.parent_issue_name
  , COALESCE(i.points_qty, CEILING(p2.parent_points_qty / p2.total_issues)) AS issue_points_qty
  , done_dt
  , assignee_name
  , assignee_on_testrail
  , reporter_name
FROM vo_issues i
LEFT ANTI JOIN vo_issues_parents_lst p
ON i.issue_name = p.parent_issue_name
LEFT JOIN vo_issues_parents_lst p2
ON i.parent_issue_name = p2.parent_issue_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Testrail Data

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vo_testrail_tests_raw AS
SELECT
    project_id AS testrail_project
  , test_id
  , datawarehouse
  , status_id
  , UPPER(TRIM(TRANSLATE(assigned_to_name, 'ÁáÉéÍíÓóÚú', 'AaEeIiOoUu'))) AS assigned_to_name
  , assigned_to_mail
  , last_test_update
FROM dv_schema.testrail_data
WHERE UPPER(status_id) <> 'UNTESTED'
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Get Data per Assignee

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dates AS
SELECT (CASE WHEN '$dateToProcess' <> 'null' THEN DATE('$dateToProcess') ELSE DATE_ADD(CURRENT_DATE, -1) END) AS process_dt
;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW test_testrail AS
SELECT
  (SELECT process_dt FROM dates) AS process_dt
  , t.assigned_to_name AS assignee_name
  , p.assigned_to_mail AS assignee_mail
  , p.role_name AS role_name
  , p.main_jira_project
  , s.start_dt AS sprint_start_dt
  , s.final_date AS sprint_complete_dt
  , ARRAY_JOIN(s.sprints_started,'; ') AS sprints_started
  , 'Testrail Test' AS task_source_type
  , CONCAT('T', t.test_id) AS task_name
  , t.datawarehouse AS task_type_name
  , CASE WHEN t.testrail_project = p.main_jira_project THEN 1 ELSE 0 END AS work_on_main_project_ind
  , testrail_project AS game_name
  , last_test_update AS task_complete_dt
  , NULL AS issue_points_qty
FROM vo_testrail_tests_raw t
INNER JOIN vo_testrail_users_main_jira_projects p
  ON t.assigned_to_name = p.testrail_user_name
INNER JOIN vo_unique_sprints s
  ON p.main_jira_project = s.project_name_long
  AND t.last_test_update >= s.start_dt 
  AND t.last_test_update <= s.final_date
;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW test_jira AS
SELECT
  (SELECT process_dt FROM dates) AS process_dt
  , i.assignee_name
  , p.assigned_to_mail AS assignee_mail
  , p.role_name AS role_name
  , p.main_jira_project
  , s.start_dt AS sprint_start_dt
  , s.final_date AS sprint_complete_dt
  , ARRAY_JOIN(s.sprints_started,'; ') AS sprints_started
  , 'Jira Issue' AS task_source_type
  , i.issue_name AS task_name
  , i.issue_type_name AS task_type_name
  , CASE WHEN i.game_name_long = p.main_jira_project THEN 1 ELSE 0 END AS work_on_main_project_ind
  , i.game_name_long AS game_name
  , i.done_dt AS task_complete_dt 
  , i.issue_points_qty
FROM vo_unit_issues i
INNER JOIN vo_testrail_users_main_jira_projects p
  ON i.assignee_name = p.testrail_user_name
INNER JOIN vo_unique_sprints s
  ON p.main_jira_project = s.project_name_long
  AND i.done_dt >= s.start_dt 
  AND i.done_dt <= s.final_date
;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW test_final AS
SELECT * FROM test_jira

UNION ALL

SELECT * FROM test_testrail

-- COMMAND ----------

-- MAGIC %python
-- MAGIC deltaLocation= 'NEW_TABLE_LOCATION'
-- MAGIC deltaDf = spark.sql("SELECT * FROM test_final")
-- MAGIC (deltaDf
-- MAGIC   .write
-- MAGIC   .format("delta")
-- MAGIC   .mode("overwrite")
-- MAGIC   .option("overwriteSchema", True)
-- MAGIC   .save(deltaLocation)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC createDelta = "CREATE TABLE IF NOT EXISTS dv_schema.jira_testrail_qa_user_summary USING delta LOCATION 'NEW_TABLE_LOCATION'"
-- MAGIC spark.sql(createDelta)
