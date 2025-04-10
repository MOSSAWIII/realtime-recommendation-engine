-- Sample users
INSERT INTO utilisateur (user_id, locale, birthyear, gender, joinedat, location, timezone, is_generated)
VALUES 
  (1, 'en_US', 1985, 'M', '2024-01-01', 'New York', -5, false),
  (2, 'en_US', 1990, 'F', '2024-01-02', 'Los Angeles', -8, false),
  (3, 'en_GB', 1988, 'F', '2024-01-03', 'London', 0, false);

-- Sample events
INSERT INTO evenement (event_id, user_id, start_time, city, state, country, name, description)
VALUES 
  (101, 1, '2025-05-01 09:00:00', 'New York', 'NY', 'USA', 'Tech Conference', 'Annual technology conference'),
  (102, 2, '2025-06-15 18:00:00', 'Los Angeles', 'CA', 'USA', 'Summer Concert', 'Outdoor music festival'),
  (103, 3, '2025-05-10 19:00:00', 'London', 'England', 'UK', 'Book Club Meeting', 'Monthly book discussion');

-- Sample friendships
INSERT INTO amitie (user_id, friend_id) 
VALUES (1, 2), (1, 3), (2, 1), (3, 1);

-- Sample event participations
INSERT INTO participation_evenement (event_id, yes_list, maybe_list, invited_list, no_list)
VALUES 
  (101, '1,2', '3', null, null),
  (102, '2', '1,3', null, null),
  (103, '3', '1', '2', null);