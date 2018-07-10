CREATE TABLE IF NOT EXISTS annotation (
  addr     TEXT,
  router   TEXT,
  asn      INT,
  conn_asn INT,
  org      TEXT,
  conn_org TEXT,
  iasn     INT,
  iorg     TEXT,
  utype    INT,
  itype    INT,
  rtype    INT
);

CREATE TABLE IF NOT EXISTS aslinks (
  addr      TEXT,
  router    TEXT,
  asn       INT,
  conn_asns TEXT
);