CREATE TYPE review_summary AS (
    total_reviews INT,
    percent_positive INT,
    review_score INT
);


CREATE TYPE weighted_tagid AS (
    tagid INT,
    weight INT
);


CREATE TYPE weighted_tagname AS (
    tagname TEXT,
    weight INT
);


CREATE TABLE IF NOT EXISTS apps (
    appid INT PRIMARY KEY,
    name TEXT,
    reviews review_summary,
    release_date TIMESTAMP,
    tagids weighted_tagid[],
    publishers TEXT[],
    developers TEXT[],
    price numeric
);



CREATE TABLE IF NOT EXISTS tags (
    tagid INT PRIMARY KEY,
    tagname TEXT
);



CREATE OR REPLACE VIEW apps_view AS
SELECT
    a.*,
    a.price * (a.reviews).total_reviews * 24.5 AS revenue_estimate,
    (
        SELECT array_agg(
            ROW(t.tagname, wt.weight)::weighted_tagname
        )
        FROM unnest(a.tagids) AS wt
        JOIN tags t ON t.tagid = wt.tagid
    ) AS tagnames
FROM apps a;


CREATE OR REPLACE FUNCTION tags_filter(
    tagids weighted_tagid[],
    whitelist int[],
    blacklist int[]
)
RETURNS boolean AS $$
DECLARE
    whitelist_count int;
BEGIN
    -- If whitelist is provided, ALL whitelist tags must be present
    IF whitelist IS NOT NULL THEN
        SELECT COUNT(DISTINCT t.tagid)
        INTO whitelist_count
        FROM unnest(tagids) AS t
        WHERE t.tagid = ANY(whitelist);

        IF whitelist_count <> array_length(whitelist, 1) THEN
            RETURN false;
        END IF;
    END IF;

    -- If whitelist is provided, NONE of these may be present
    IF blacklist IS NOT NULL THEN
        IF EXISTS (
            SELECT 1 
            FROM unnest(tagids) AS t
            WHERE t.tagid = ANY(blacklist)
        ) THEN
            RETURN false;
        END IF;
    END IF;

    RETURN true;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
