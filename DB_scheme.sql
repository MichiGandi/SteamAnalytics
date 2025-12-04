CREATE TYPE review_summary AS (
    total_reviews INT,
    percent_positive INT,
    review_score INT
);


CREATE TYPE weighted_tag AS (
    tagid INT,
    weight INT
);


CREATE TABLE apps (
    appid INT PRIMARY KEY,
    name TEXT,
    reviews review_summary,
    release_date TIMESTAMP,
    tags weighted_tag[],
    publishers TEXT[],
    developers TEXT[],
    price numeric
);


CREATE VIEW apps_view AS
SELECT
    *,
    price * (reviews).total_reviews * 24.5 AS revenue_estimate
FROM apps;
