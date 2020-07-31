SELECT
  TO_CHAR(ts, 'yyyy-MM') AS Dateformat,
  COUNT(DISTINCT userid)
FROM adhoc.jayone_test
GROUP BY Dateformat
ORDER BY Dateformat;