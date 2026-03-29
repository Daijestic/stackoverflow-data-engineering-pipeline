-- analytics_queries.sql
-- Project: StackOverflow Data Engineering Pipeline
-- Assumption:
--   - fact_questions.tags is stored as text[] in PostgreSQL
--   - Gold tables already exist:
--       fact_questions
--       fact_answers
--       agg_question_acceptance
--       agg_daily_activity

-- =========================================================
-- 1) Ngày nào có nhiều question nhất
-- Grain nguồn: 1 dòng = 1 ngày
-- Bảng tối ưu: agg_daily_activity
-- =========================================================
SELECT
    date,
    total_questions
FROM agg_daily_activity
ORDER BY total_questions DESC, date ASC
LIMIT 10;


-- =========================================================
-- 2) Ngày nào có acceptance rate cao nhất
-- Lọc total_answers > 0 để tránh ngày không có answer
-- =========================================================
SELECT
    date,
    total_answers,
    accepted_answers,
    accepted_answer_rate_daily
FROM agg_daily_activity
WHERE total_answers > 0
ORDER BY accepted_answer_rate_daily DESC, total_answers DESC, date ASC
LIMIT 10;


-- =========================================================
-- 3) Top 10 questions có nhiều answer nhất nhưng acceptance rate thấp
-- Dùng bảng aggregate để reuse metric, join fact_questions để lấy title/tags/view_count
-- =========================================================
SELECT
    q.question_id,
    q.title,
    q.tags,
    q.view_count,
    qa.total_answers,
    qa.accepted_answers,
    qa.accepted_answer_rate
FROM fact_questions q
JOIN agg_question_acceptance qa
    ON q.question_id = qa.question_id
WHERE qa.accepted_answer_rate < 0.30
ORDER BY qa.total_answers DESC, qa.accepted_answer_rate ASC
LIMIT 10;


-- =========================================================
-- 4) Tag nào có nhiều question nhất
-- Cần unnest tags để đổi grain từ question -> question_tag
-- =========================================================
SELECT
    tag,
    COUNT(*) AS total_questions_in_tag
FROM (
    SELECT
        question_id,
        UNNEST(tags) AS tag
    FROM fact_questions
) q_tag
GROUP BY tag
ORDER BY total_questions_in_tag DESC, tag ASC
LIMIT 20;


-- =========================================================
-- 5) Top 5 tag có acceptance rate cao nhất
-- Logic:
--   - explode/unnest tags từ fact_questions
--   - join fact_answers theo question_id
--   - group theo tag
--   - tính total_answers, accepted_answers, accepted_answer_rate
-- Lọc total_answers > 0 để tránh chia cho 0
-- =========================================================
SELECT
    q_tag.tag,
    COUNT(*) AS total_answers,
    SUM(CASE WHEN a.is_accepted THEN 1 ELSE 0 END) AS accepted_answers,
    ROUND(
        SUM(CASE WHEN a.is_accepted THEN 1 ELSE 0 END)::numeric
        / NULLIF(COUNT(*), 0),
        4
    ) AS accepted_answer_rate
FROM (
    SELECT
        question_id,
        UNNEST(tags) AS tag
    FROM fact_questions
) q_tag
JOIN fact_answers a
    ON q_tag.question_id = a.question_id
GROUP BY q_tag.tag
HAVING COUNT(*) > 0
ORDER BY accepted_answer_rate DESC, total_answers DESC, q_tag.tag ASC
LIMIT 5;


-- =========================================================
-- 6) Bonus: Top 10 questions nhiều view nhưng ít answer
-- Hữu ích để tìm question có tiềm năng nhưng chưa được trả lời tốt
-- =========================================================
SELECT
    question_id,
    title,
    tags,
    view_count,
    answer_count,
    score,
    is_answered
FROM fact_questions
WHERE view_count IS NOT NULL
  AND answer_count IS NOT NULL
ORDER BY view_count DESC, answer_count ASC
LIMIT 10;


-- =========================================================
-- 7) Bonus: Top 10 questions có nhiều answer nhất nhưng chưa có accepted answer
-- Nếu fact_questions có accepted_answer_id
-- =========================================================
SELECT
    question_id,
    title,
    tags,
    view_count,
    answer_count,
    score
FROM fact_questions
WHERE accepted_answer_id IS NULL
ORDER BY answer_count DESC, view_count DESC
LIMIT 10;