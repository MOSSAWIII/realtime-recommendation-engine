-- 1. Drop existing table
DROP TABLE IF EXISTS RESEAU_AMIS CASCADE;

-- 2. Create table
CREATE TABLE RESEAU_AMIS (
    user_id BIGINT PRIMARY KEY REFERENCES UTILISATEUR(user_id),
    friends_list TEXT
);

-- 3. Create temporary table
CREATE TEMPORARY TABLE temp_friends (
    user_id TEXT,
    friend_id TEXT
);

-- 4. Import data
\COPY temp_friends FROM '/Users/mohamed.el.moussaoui/Desktop/Real_time_rec_sys/realtime-recommendation-engine/database/user_friends.csv' WITH CSV HEADER DELIMITER ',';

-- 5. Insert aggregated data
INSERT INTO RESEAU_AMIS (user_id, friends_list)
SELECT 
    user_id::BIGINT,
    string_agg(friend_id, ' ') as friends_list
FROM (
    SELECT DISTINCT user_id, friend_id 
    FROM temp_friends
) as unique_friends
GROUP BY user_id;

-- 6. Clean up
DROP TABLE IF EXISTS temp_friends;

-- 7. Create index sur la longueur plutôt que le contenu
-- Index sur la longueur est plus petit et ne dépasse pas la limite de 8191 octets
CREATE INDEX idx_reseau_amis_friends_length ON RESEAU_AMIS(length(friends_list));

-- 8. Optionnel: création d'index GIN pour recherche dans le texte (nécessite pg_trgm)
-- Décommenter si nécessaire pour recherche textuelle
-- CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- CREATE INDEX idx_reseau_amis_friends_gin ON RESEAU_AMIS USING GIN (friends_list gin_trgm_ops);

ANALYZE RESEAU_AMIS;

