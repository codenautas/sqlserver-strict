


DROP TABLE IF EXISTS perfect_nums;
go;

CREATE TABLE perfect_nums(
    num bigint
);

INSERT INTO perfect_nums values (6), (28), (496), (8128);
go;

SELECT count(*) FROM perfect_nums;