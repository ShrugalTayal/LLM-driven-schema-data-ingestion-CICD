CREATE TABLE IF NOT EXISTS bfsi_global_cybersecurity_threats_2015_2024 (
  country TEXT NOT NULL,
  year BIGINT NOT NULL,
  attack_type TEXT NOT NULL,
  target_industry TEXT NOT NULL,
  financial_loss__in_million___ DOUBLE PRECISION NOT NULL,
  number_of_affected_users BIGINT NOT NULL,
  attack_source TEXT NOT NULL,
  security_vulnerability_type TEXT NOT NULL,
  defense_mechanism_used TEXT NOT NULL,
  incident_resolution_time__in_hours_ BIGINT NOT NULL
);