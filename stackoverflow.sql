/* The Stack Overflow dataset is a comprehensive collection of data extracted from Stack Overflow,
a popular online community for programmers to ask and answer questions. This dataset includes information about users, 
questions, answers, tags, comments, badges, and more. It can be used to perform various types of analyses related to user behavior, 
trends in programming languages, and other aspects of software development. Here I am trying to break the dataset to gain some insight 
*/

-- List all the questions and all its answers for any questions related to Python programming language in 2020.
SELECT
  q.id AS q_id,
  q.title AS question,
  q.body AS question_body,
  q.owner_user_id AS question_owner_userid,
  q.accepted_answer_id,
  a.id AS a_id,
  a.body AS answer,
  a.score AS answer_score,
  a.owner_user_id AS answerer_owner_userid,
  q.creation_date AS date,
  q.tags AS tags
FROM
  `bigquery-public-data.stackoverflow.posts_questions` q
LEFT JOIN
  `bigquery-public-data.stackoverflow.posts_answers` a
ON a.parent_id = q.id
WHERE 
 q.tags LIKE '%python%'
 AND EXTRACT(YEAR FROM q.creation_date) = 2020
ORDER BY q_id, a_id;

-- How many questions do all Indonesian people create per year starting from 2015 to 2020?
SELECT
  EXTRACT(YEAR FROM q.creation_date) AS year,
  COUNT(q.id) AS total_questions
FROM
  `bigquery-public-data.stackoverflow.posts_questions` q
JOIN
  `bigquery-public-data.stackoverflow.users` u
ON u.id = q.owner_user_id
WHERE
  u.location LIKE '%indonesia%'
  AND EXTRACT(YEAR FROM q.creation_date) BETWEEN 2015 AND 2020
GROUP BY
  year
ORDER BY
  year;

-- Which usernames that got badges in Jan 2020? please create unique ranking based on when they got the badge per class
WITH BadgeRanking AS (
  SELECT
    b.user_id,
    b.name AS badge_name,
    b.tag_based,
    b.class AS badge_class,
    b.date AS badge_date,
    ROW_NUMBER() OVER (PARTITION BY b.class ORDER BY b.date) AS badge_rank
  FROM
    `bigquery-public-data.stackoverflow.badges` b
  WHERE
    EXTRACT(YEAR FROM b.date) = 2020
    AND EXTRACT(MONTH FROM b.date) = 1
)
SELECT
  u.display_name AS username,
  r.badge_name,
  r.tag_based,
  r.badge_class,
  r.badge_date,
  r.badge_rank
FROM
  BadgeRanking r
JOIN
  `bigquery-public-data.stackoverflow.users` u
ON u.id = r.user_id
ORDER BY
  r.badge_class, r.badge_rank;

-- Which usernames that got badges in Jan 2020? please create unique ranking based on when they got the badge per class
WITH YearlyAnswerRanking AS (
  SELECT
    EXTRACT(YEAR FROM q.creation_date) AS year,
    COUNT(a.id) AS answer_count,
    RANK() OVER (ORDER BY COUNT(a.id) DESC) AS answer_rank
  FROM
    `bigquery-public-data.stackoverflow.posts_questions` q
  LEFT JOIN
    `bigquery-public-data.stackoverflow.posts_answers` a
  ON q.id = a.parent_id
  WHERE
    q.tags LIKE '%bigquery%'
  GROUP BY
    year
)
SELECT
  year,
  answer_count,
  answer_rank
FROM
  YearlyAnswerRanking
ORDER BY
  year;

-- For people who have answers at least 5000 answers, what is the average time span in hours do each of them spend answering questions?
WITH UserAnswerTime AS (
  SELECT
    a.owner_user_id,
    a.creation_date AS answer_date,
    LAG(a.creation_date) OVER (PARTITION BY a.owner_user_id ORDER BY a.creation_date) AS prev_answer_date
  FROM
    `bigquery-public-data.stackoverflow.posts_answers` a
),
UserAnswerCounts AS (
  SELECT
    owner_user_id,
    COUNT(*) AS answer_count
  FROM
    UserAnswerTime
  GROUP BY
    owner_user_id
)
SELECT
  u.display_name AS username,
  u.creation_date AS user_creation_date,
  c.answer_count,
  AVG(TIMESTAMP_DIFF(answer_date, prev_answer_date, HOUR)) AS avg_time_span_hours
FROM
  UserAnswerTime a
JOIN
  UserAnswerCounts c
ON a.owner_user_id = c.owner_user_id
JOIN
  `bigquery-public-data.stackoverflow.users` u
ON u.id = a.owner_user_id
WHERE
  c.answer_count >= 5000
GROUP BY
  username, user_creation_date, answer_count
ORDER BY
  avg_time_span_hours DESC;

-- Top 10 location where users are most located
SELECT
  location,
  COUNT(*) AS user_count
FROM
  `bigquery-public-data.stackoverflow.users`
WHERE
  location IS NOT NULL
GROUP BY
  location
ORDER BY
  user_count DESC;

-- Number of questions a person might create based on their number of active years
SELECT
  active_years,
  APPROX_QUANTILES(question_count, 5) AS percentiles,
  APPROX_QUANTILES(question_count, 2)[SAFE_ORDINAL(2)] AS median
FROM (
  SELECT
    u.id AS user_id,
    COUNT(q.id) AS question_count,
    EXTRACT(YEAR FROM MAX(q.last_activity_date)) - EXTRACT(YEAR FROM MIN(q.creation_date)) AS active_years
  FROM
    `bigquery-public-data.stackoverflow.users` u
  JOIN
    `bigquery-public-data.stackoverflow.posts_questions` q
  ON u.id = q.owner_user_id
  GROUP BY
    u.id
)
GROUP BY
  active_years
ORDER BY
  active_years;
