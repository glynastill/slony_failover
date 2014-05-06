CREATE USER failover_log_user WITH PASSWORD 'failover_log_password';

DROP TABLE IF EXISTS public.failovers;
CREATE TABLE public.failovers
(
  id bigserial NOT NULL PRIMARY KEY,
  applied timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  reason varchar,
  exit_code integer,
  results text,
  script text,
  cluster_name varchar
);
GRANT SELECT, INSERT ON TABLE public.failovers TO failover_log_user;
GRANT SELECT, UPDATE ON TABLE public.failovers_id_seq TO failover_log_user;
