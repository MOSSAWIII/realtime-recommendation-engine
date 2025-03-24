
-- 1. Drop existing table
DROP TABLE IF EXISTS UTILISATEUR CASCADE;

-- 2. Create table with proper structure
CREATE TABLE UTILISATEUR (
    user_id BIGINT PRIMARY KEY,
    locale VARCHAR(10),
    birthyear FLOAT,
    gender VARCHAR(10),
    joinedAt TIMESTAMP, 
    location VARCHAR(100),
    timezone INT
);

-- 3. Create temporary table for import
CREATE TEMPORARY TABLE temp_users (
    user_id TEXT,
    locale TEXT,
    birthyear TEXT,
    gender TEXT,
    joinedAt TEXT,
    location TEXT,
    timezone TEXT
);

-- 4. Import data
\COPY temp_users FROM '/Users/mohamed.el.moussaoui/Desktop/Real_time_rec_sys/realtime-recommendation-engine/database/users.csv' WITH CSV HEADER DELIMITER ',';

-- 5. Insert validated data with better filtering
INSERT INTO UTILISATEUR (user_id, locale, birthyear, gender, joinedAt, location, timezone)
SELECT 
    user_id::BIGINT, 
    NULLIF(locale, 'None'),
    CASE 
        WHEN birthyear = 'None' OR birthyear = '' THEN NULL
        WHEN birthyear ~ '^[0-9]+(\.[0-9]+)?$' THEN birthyear::FLOAT
        ELSE NULL
    END AS birthyear,
    NULLIF(gender, 'None'),
    CASE WHEN joinedAt = 'None' OR joinedAt = '' THEN NULL ELSE joinedAt::TIMESTAMP END,
    NULLIF(location, 'None'),
    CASE WHEN timezone = 'None' OR timezone = '' THEN NULL ELSE timezone::INT END
FROM temp_users
WHERE user_id != 'None' AND user_id != '' AND user_id ~ '^[0-9]+$';

-- 6. Clean up
DROP TABLE IF EXISTS temp_users;

-- 7. Create index
CREATE INDEX idx_utilisateur_location ON UTILISATEUR(location);

ANALYZE UTILISATEUR;
