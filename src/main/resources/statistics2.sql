CREATE EXTENSION btree_gin;
CREATE STATISTICS employee_stats ON ssn, dno, salary, fname, sex FROM employee;
CREATE STATISTICS projects_stats ON pnumber, dnumber FROM project;
CREATE STATISTICS works_on_stats ON essn, pno FROM works_on;
CREATE STATISTICS dependent_stats ON essn, dependent_name, sex FROM dependent;