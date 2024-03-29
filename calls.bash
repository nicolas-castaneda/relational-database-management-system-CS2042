curl -d "{\"query\": \"CREATE TABLE test(id int primary key, col1 char(50), mode int, val double);\"}" -X POST http://localhost:8080/query --verbose
curl -d "{\"query\": \"INSERT INTO test VALUES (25,'comida',8,5.9);\"}" -X POST http://localhost:8080/query --verbose
curl -d "{\"query\": \"INSERT INTO test VALUES (26,'comida',8,5.9);\"}" -X POST http://localhost:8080/query --verbose