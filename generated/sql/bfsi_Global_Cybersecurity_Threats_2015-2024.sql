CREATE TABLE IF NOT EXISTS bfsi_Global_Cybersecurity_Threats_2015-2024 (
  Country object NOT NULL,
  Year int64 NOT NULL,
  Attack Type object NOT NULL,
  Target Industry object NOT NULL,
  Financial Loss (in Million $) float64 NOT NULL,
  Number of Affected Users int64 NOT NULL,
  Attack Source object NOT NULL,
  Security Vulnerability Type object NOT NULL,
  Defense Mechanism Used object NOT NULL,
  Incident Resolution Time (in Hours) int64 NOT NULL
);