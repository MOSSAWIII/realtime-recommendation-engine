-- 1. Drop existing table
DROP TABLE IF EXISTS PARTICIPATION_EVENEMENT CASCADE;

-- 2. Create table
CREATE TABLE PARTICIPATION_EVENEMENT (
    event_id BIGINT PRIMARY KEY REFERENCES EVENEMENT(event_id),
    yes_list TEXT NULL,
    maybe_list TEXT NULL,
    invited_list TEXT NULL,
    no_list TEXT NULL
);

-- 3. Create temporary table avec la même structure que le CSV
CREATE TEMPORARY TABLE temp_attendees (
    event_id TEXT,
    yes_list TEXT,
    maybe_list TEXT,
    invited_list TEXT,
    no_list TEXT
);

-- 4. Import data avec la structure correcte du CSV
\COPY temp_attendees FROM '/Users/mohamed.el.moussaoui/Desktop/Real_time_rec_sys/realtime-recommendation-engine/database/event_attendees.csv' WITH CSV HEADER DELIMITER ',' NULL '';

-- 5. Insert data (direct mapping)
INSERT INTO PARTICIPATION_EVENEMENT (event_id, yes_list, maybe_list, invited_list, no_list)
SELECT 
    event_id::BIGINT,
    yes_list,
    maybe_list,
    invited_list,
    no_list
FROM temp_attendees;

-- 6. Clean up
DROP TABLE IF EXISTS temp_attendees;

-- 7. Create indexes sur les longueurs plutôt que les contenus
CREATE INDEX idx_participation_yes_length ON PARTICIPATION_EVENEMENT(length(yes_list));
CREATE INDEX idx_participation_maybe_length ON PARTICIPATION_EVENEMENT(length(maybe_list));
CREATE INDEX idx_participation_invited_length ON PARTICIPATION_EVENEMENT(length(invited_list));
CREATE INDEX idx_participation_no_length ON PARTICIPATION_EVENEMENT(length(no_list));

-- 8. Optionnel: création d'index GIN pour recherche dans le texte
-- Décommenter si nécessaire pour recherche textuelle
-- CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- CREATE INDEX idx_participation_yes_gin ON PARTICIPATION_EVENEMENT USING GIN (yes_list gin_trgm_ops);
-- CREATE INDEX idx_participation_invited_gin ON PARTICIPATION_EVENEMENT USING GIN (invited_list gin_trgm_ops);

-- 9. Analyze
ANALYZE PARTICIPATION_EVENEMENT;

