-- Insert station availability snapshot

INSERT INTO station_availability
(station_id, num_bikes_available, num_bikes_mechanical, num_bikes_ebike,
 num_docks_available, is_installed, is_renting, is_returning, last_reported)
VALUES %s
