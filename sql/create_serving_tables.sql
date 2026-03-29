-- create_serving_tables.sql
-- Project: StackOverflow Data Engineering Pipeline
-- Serving layer schema for PostgreSQL analytics

-- =========================================================
-- 1) FACT QUESTIONS
-- Grain: 1 row = 1 question
-- =========================================================
CREATE TABLE IF NOT EXISTS fact_questions (
    question_id BIGINT PRIMARY KEY,
    title TEXT,
    tags TEXT[],
    creation_date TIMESTAMP,
    date DATE,
    score INT,
    view_count INT,
    answer_count INT,
    is_answered BOOLEAN,
    accepted_answer_id BIGINT
);

CREATE INDEX IF NOT EXISTS idx_fact_questions_date
    ON fact_questions(date);

CREATE INDEX IF NOT EXISTS idx_fact_questions_view_count
    ON fact_questions(view_count);

CREATE INDEX IF NOT EXISTS idx_fact_questions_answer_count
    ON fact_questions(answer_count);


-- =========================================================
-- 2) FACT ANSWERS
-- Grain: 1 row = 1 answer
-- =========================================================
CREATE TABLE IF NOT EXISTS fact_answers (
    answer_id BIGINT PRIMARY KEY,
    question_id BIGINT NOT NULL,
    creation_date TIMESTAMP,
    last_activity_date TIMESTAMP,
    date DATE,
    score INT,
    is_accepted BOOLEAN,
    owner_user_id BIGINT,
    owner_display_name TEXT,
    owner_reputation INT
);

CREATE INDEX IF NOT EXISTS idx_fact_answers_question_id
    ON fact_answers(question_id);

CREATE INDEX IF NOT EXISTS idx_fact_answers_date
    ON fact_answers(date);

CREATE INDEX IF NOT EXISTS idx_fact_answers_is_accepted
    ON fact_answers(is_accepted);


-- =========================================================
-- 3) AGG QUESTION ACCEPTANCE
-- Grain: 1 row = 1 question
-- =========================================================
CREATE TABLE IF NOT EXISTS agg_question_acceptance (
    question_id BIGINT PRIMARY KEY,
    total_answers INT NOT NULL,
    accepted_answers INT NOT NULL,
    accepted_answer_rate NUMERIC(10,4) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agg_question_acceptance_total_answers
    ON agg_question_acceptance(total_answers);

CREATE INDEX IF NOT EXISTS idx_agg_question_acceptance_rate
    ON agg_question_acceptance(accepted_answer_rate);


-- =========================================================
-- 4) AGG DAILY ACTIVITY
-- Grain: 1 row = 1 day
-- =========================================================
CREATE TABLE IF NOT EXISTS agg_daily_activity (
    date DATE PRIMARY KEY,
    total_questions INT NOT NULL,
    total_answers INT NOT NULL,
    accepted_answers INT NOT NULL,
    accepted_answer_rate_daily NUMERIC(10,4) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agg_daily_activity_total_questions
    ON agg_daily_activity(total_questions);

CREATE INDEX IF NOT EXISTS idx_agg_daily_activity_rate
    ON agg_daily_activity(accepted_answer_rate_daily);