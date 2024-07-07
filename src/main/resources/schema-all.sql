DROP TABLE IF EXISTS people;
DROP TABLE IF EXISTS imp_people;

CREATE TABLE people (
    person_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(20),
    last_name VARCHAR(20)
);

CREATE TABLE imp_people (
    person_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(20),
    last_name VARCHAR(20)
);
