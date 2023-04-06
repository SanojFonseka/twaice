/*-------------------------------------Task 3--------------------------------------------------*/
/* Change the query below so that it doesn't contain any correlated subqueries
/* Instructions: Create your own table structure with the provided data for testing purposes. You can work with a data sample.
/*---------------------------------------------------------------------------------------*/


SELECT SUM(m1.m_voltage) 
FROM measurements m1
JOIN (
    SELECT m_container_id, AVG(m_voltage) AS avg_m_voltage 
    FROM measurements
    GROUP BY m_container_id
) AS m2
ON m1.m_container_id = m2.m_container_id AND m1.m_voltage > m2.avg_m_voltage;