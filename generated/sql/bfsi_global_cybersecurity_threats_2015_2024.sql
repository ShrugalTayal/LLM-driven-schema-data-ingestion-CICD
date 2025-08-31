CREATE TABLE IF NOT EXISTS bfsi_global_cybersecurity_threats_2015_2024 (
  Country TEXT NOT NULL,
  Year BIGINT NOT NULL,
  Attack Type TEXT NOT NULL,
  Target Industry TEXT NOT NULL,
  Financial Loss (in Million $) DOUBLE PRECISION NOT NULL,
  Number of Affected Users BIGINT NOT NULL,
  Attack Source TEXT NOT NULL,
  Security Vulnerability Type TEXT NOT NULL,
  Defense Mechanism Used TEXT NOT NULL,
  Incident Resolution Time (in Hours) BIGINT NOT NULL
);