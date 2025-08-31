CREATE TABLE IF NOT EXISTS bfsi_global_cybersecurity_threats_2015_2024 (
  country object NOT NULL,
  year int64 NOT NULL,
  attack_type object NOT NULL,
  target_industry object NOT NULL,
  financial_loss__in_million___ float64 NOT NULL,
  number_of_affected_users int64 NOT NULL,
  attack_source object NOT NULL,
  security_vulnerability_type object NOT NULL,
  defense_mechanism_used object NOT NULL,
  incident_resolution_time__in_hours_ int64 NOT NULL
);