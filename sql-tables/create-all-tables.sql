-- Master script to create all tables and indexes for EventRecommendationEngine

-- Importation des utilisateurs
BEGIN;
SET session_replication_role = 'replica';
\i import-users.sql
SET session_replication_role = 'origin';
COMMIT;

-- Importation des événements
BEGIN;
SET session_replication_role = 'replica';
\i import-events.sql
SET session_replication_role = 'origin';
COMMIT;

-- Importation des amis
BEGIN;
SET session_replication_role = 'replica';
\i import-friends.sql
SET session_replication_role = 'origin';
COMMIT;

-- Importation des participations
BEGIN;
SET session_replication_role = 'replica';
\i import-event-attendees.sql
SET session_replication_role = 'origin';
COMMIT;

-- Affichage des résultats
SELECT 'UTILISATEUR' as table_name, COUNT(*) as record_count FROM UTILISATEUR
UNION
SELECT 'EVENEMENT', COUNT(*) FROM EVENEMENT
UNION
SELECT 'RESEAU_AMIS', COUNT(*) FROM RESEAU_AMIS
UNION
SELECT 'PARTICIPATION_EVENEMENT', COUNT(*) FROM PARTICIPATION_EVENEMENT
ORDER BY table_name;

ANALYZE VERBOSE;