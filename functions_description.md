## Таблицы

### Задание
Создайте таблицу `employees` с полями: `emp_id` типа `SERIAL`, `emp_name` типа `VARCHAR(100)`, `position` типа `VARCHAR(50)` и `salary` типа `DECIMAL(10, 2)`. Установите первичный ключ по полю `emp_id`.

### Решение
```sql
DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
    emp_id SERIAL PRIMARY KEY,
    emp_name VARCHAR(100),
    position VARCHAR(50),
    salary DECIMAL(10, 2)
);
```
### Проверочная функция check_table_structure()
На вход подаются:
+ название таблицы
+ наименование колонки для первичного ключа
+ описание всех колонок в формате JSON (название; тип; для VARCHAR, CHAR и DECIMAL - размер)

Функция вернёт:
+ 0.2 балла за существование таблицы с правильным названием
+ 0.2 балла за правильный первичный ключ
+ по 0.6/n баллов за каждую правильную колонку, где n - количество колонок
Если таблицы не существует, сразу возвращается 0. Результат округляется до десятых.
```sql
CREATE OR REPLACE FUNCTION check_table_structure(
    table_name TEXT,
    pk_column TEXT,
    columns_to_check JSONB
) RETURNS NUMERIC AS $$
DECLARE
    col_element JSONB;
    check_result BOOLEAN;
    column_check_query TEXT;
    total_score NUMERIC := 0.0;
    column_score NUMERIC := 0.0;
    column_count INT := 0;
BEGIN
    -- Проверка существования таблицы
    EXECUTE format('
        SELECT EXISTS (
            SELECT 1
            FROM pg_tables
            WHERE tablename = %L
        )', table_name)
    INTO check_result;
    
    IF NOT check_result THEN
        RETURN 0.0;
    END IF;
    total_score := total_score + 0.2;

    -- Проверка первичного ключа
    EXECUTE format('
        SELECT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = %L::regclass
            AND contype = ''p''
            AND pg_get_constraintdef(oid) LIKE %L
        )', table_name, '%PRIMARY KEY (' || pk_column || '%')
    INTO check_result;
    
    IF check_result THEN
        total_score := total_score + 0.2;
    END IF;

    column_count := jsonb_array_length(columns_to_check);

    -- Если колонок нет, возвращаем текущий балл
    IF column_count = 0 THEN
        RETURN round(total_score, 1);
    END IF;

    -- Рассчитываем вес каждой колонки
    column_score := 0.6 / column_count;

    -- Проверка колонок
    FOR col_element IN
        SELECT * FROM jsonb_array_elements(columns_to_check)
    LOOP
        column_check_query = format('
            SELECT EXISTS (
                SELECT 1
                FROM pg_attribute a
                JOIN pg_type t ON t.oid = a.atttypid
                WHERE a.attrelid = %L::regclass
                AND a.attname = %L
                AND t.typname = %L',
            table_name,
            col_element->>'name',
            col_element->>'type');

        -- Проверка дополнительных параметров для varchar, char и numeric
        CASE 
            WHEN col_element->>'type' in ('varchar','char', 'bpchar') THEN
                column_check_query = column_check_query || format('
                    AND a.atttypmod - 4 = %s)',
                    (col_element->'params'->>'length')::INT);

            WHEN col_element->>'type' in ('numeric') THEN
                column_check_query = column_check_query || format('
                    AND (a.atttypmod >> 16) & 65535 = %s 
                    AND (a.atttypmod - 4) & 65535 = %s)',
                    (col_element->'params'->>'precision')::INT,
                    (col_element->'params'->>'scale')::INT);
            ELSE
                -- Для остальных типов проверяем только имя и тип
                column_check_query = column_check_query || ')';
        END CASE;

        EXECUTE column_check_query INTO check_result;
        
        IF check_result THEN
            total_score := total_score + column_score;
        END IF;
    END LOOP;
    RETURN round(total_score, 1);
END;
$$ LANGUAGE plpgsql;
```
### Запуск функции
```sql
SELECT check_table_structure(
    'employees', 
    'emp_id', 
    '[{ "name": "emp_id", "type": "int4"}, 
     {"name": "emp_name", "type": "varchar", "params": {"length": 100}}, 
      {"name": "position", "type": "varchar", "params": {"length": 50}}, 
      {"name": "salary", "type": "numeric", "params": {"precision": 10, "scale": 2}}]'
);
```
## Представления
### Задание
Создать представление `max_salary_position`, отображающее максимальную зарплату по каждой позиции из таблицы `employees`. В первой колонке - `position` - должно быть указано название позиции, во второй – `max_salary` - максимальная зарплата среди сотрудников этой позиции.
### Решение
```sql
DROP TABLE IF EXISTS employees CASCADE;
CREATE TABLE employees (
	emp_id SERIAL PRIMARY KEY,
	emp_name VARCHAR(100),
	position VARCHAR(50),
	salary DECIMAL(10, 2)
);
INSERT INTO employees (emp_name, position, salary) VALUES
('Alice', 'Developer', 9000.00),
('Bob', 'Developer', 9500.00),
('Charlie', 'Manager', 12000.00),
('David', 'Manager', 11000.00),
('Eve', 'Developer', 8500.00);
DROP VIEW IF EXISTS max_salary_position;

CREATE VIEW max_salary_position AS
SELECT
    position,
    MAX(salary) AS max_salary
FROM
    employees
GROUP BY
    position;

```
### Проверочная функция check_view()
На вход подаются:
+ название представления
+ описание колонок в формате JSON (только названия)
+ ожидаемое содержимое представления (опционально, по умолчанию пусто)

Функция вернёт:
+ 0.2 балла за существование представления с правильным названием
+ 0.3 балла, если количество колонок и их имена верные
+ 0.5 балла, если содержимое представления верное
Если представления не существует, сразу возвращается 0. 
```sql
CREATE OR REPLACE FUNCTION check_view(
	view_name TEXT,         
	columns_to_check JSONB,      
	expected_data JSONB DEFAULT NULL
) RETURNS NUMERIC AS $$
DECLARE
	view_exists BOOLEAN;
	column_count INTEGER;
	actual_data JSONB;
	column_name TEXT;
	column_info JSONB;
BEGIN
    -- Проверка существования представления
    SELECT EXISTS (
    	SELECT 1
    	FROM pg_views
    	WHERE viewname = view_name
	) INTO view_exists;
 
	IF NOT view_exists THEN
    	RETURN 0.0; 	END IF;
 
    -- Проверка количества столбцов
    SELECT COUNT(*)
	INTO column_count
	FROM pg_attribute
	WHERE attrelid = view_name::regclass
  	AND attnum > 0
  	AND NOT attisdropped;
 
	IF column_count <> jsonb_array_length(columns_to_check) THEN
    	RETURN 0.2;
	END IF;
 
    	-- Проверка наличия нужных столбцов
	FOR column_info IN SELECT * FROM jsonb_array_elements(columns_to_check) LOOP
    	column_name := column_info->>'name';
 
    	IF NOT EXISTS (
        	SELECT 1
        	FROM pg_attribute
        	WHERE attrelid = view_name::regclass
          	AND attname = column_name
          	AND attnum > 0
          	AND NOT attisdropped
    	) THEN
        	RETURN 0.2;
    	END IF;
	END LOOP;
 
-- Проверка данных
	IF expected_data IS NULL THEN
    	RETURN 1.0;
	END IF;

 	EXECUTE format('SELECT jsonb_agg(row_to_json(%I)) FROM %I', view_name, view_name)
	INTO actual_data;
 
	expected_data := (
    	SELECT jsonb_agg(elem ORDER BY elem->>'position')
    	FROM jsonb_array_elements(expected_data) AS elem
	);
 
	actual_data := (
    	SELECT jsonb_agg(elem ORDER BY elem->>'position')
    	FROM jsonb_array_elements(actual_data) AS elem
	);
 
	IF actual_data::TEXT = expected_data::TEXT THEN
    	RETURN 1.0; 
	ELSE
    	RETURN 0.5;
	END IF;
 
	RETURN 0.2;
END;
$$ LANGUAGE plpgsql;

```
### Запуск функции
```sql
SELECT check_view(
    'max_salary_position', 
    '[                 
        {"name": "position"},
        {"name": "max_salary"}
    ]'::JSONB,
    '[                    
        {"position": "Developer", "max_salary": 9500.00},
        {"position": "Manager", "max_salary": 12000.00}
    ]'::JSONB
);	

```

## Материализованные представления
### Задание
Создать материализованное представление max_salary_position, отображающее максимальную зарплату по каждой позиции из таблицы employees. В первой колонке - position - должно быть указано название позиции, во второй – max_salary - максимальная зарплата среди сотрудников этой позиции.
### Решение
```sql
DROP TABLE IF EXISTS employees CASCADE;
CREATE TABLE employees (
	emp_id SERIAL PRIMARY KEY,
	emp_name VARCHAR(100),
	position VARCHAR(50),
	salary DECIMAL(10, 2)
);
INSERT INTO employees (emp_name, position, salary) VALUES
('Alice', 'Developer', 9000.00),
('Bob', 'Developer', 9500.00),
('Charlie', 'Manager', 12000.00),
('David', 'Manager', 11000.00),
('Eve', 'Developer', 8500.00);
DROP MATERIALIZED VIEW IF EXISTS max_salary_position;

CREATE MATERIALIZED VIEW max_salary_position AS
SELECT 
    position, 
    MAX(salary) AS max_salary
FROM 
    employees
GROUP BY 
    position;
```
Проверочная функция check_materialized_view()
На вход подаются:
название мат.представления
описание колонок в формате JSON (только названия)
ожидаемое содержимое мат. представления (опционально, по умолчанию пусто)

Функция вернёт:
0.2 балла за существование мат. представления с правильным названием
+ 0.3 балла, если количество колонок и их имена верные
+ 0.5 балла, если содержимое мат. представления верное
Если мат. представления не существует, сразу возвращается 0. 
```sql
CREATE OR REPLACE FUNCTION check_materialized_view(
    view_name TEXT, 
    expected_columns JSONB, 
    expected_data JSONB DEFAULT NULL
) RETURNS NUMERIC AS $$
DECLARE
    is_view_exists BOOLEAN;
    column_count INT;
    actual_data JSONB;
BEGIN
    -- Проверка существования материализованного представления
    SELECT EXISTS (
        SELECT 1
        FROM pg_matviews
        WHERE matviewname = view_name
    ) INTO is_view_exists;

    IF NOT is_view_exists THEN
        RETURN 0.0;
    END IF;

    -- Проверка количества столбцов
    SELECT COUNT(*)
    INTO column_count
    FROM pg_attribute
    WHERE attrelid = view_name::regclass
      AND attnum > 0
      AND NOT attisdropped;

    IF column_count <> jsonb_array_length(expected_columns) THEN
        RETURN 0.2;
    END IF;

    -- Проверка наличия нужных столбцов
    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(expected_columns) AS col
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_attribute a
            WHERE a.attrelid = view_name::regclass
              AND a.attname = col->>'name'
        )
    ) THEN
        RETURN 0.2;
    END IF;

    -- Проверка данных
    IF expected_data IS NOT NULL THEN
        EXECUTE format('
            SELECT jsonb_agg(row_to_json(t))
            FROM (
                SELECT * FROM %I
                ORDER BY position -- Сортировка по position
            ) t', view_name)
        INTO actual_data;

        expected_data := (
            SELECT jsonb_agg(elem ORDER BY elem->>'position')
            FROM jsonb_array_elements(expected_data) AS elem
        );
        IF actual_data::TEXT <> expected_data::TEXT THEN
            RETURN 0.5;
        END IF;
    END IF;
    RETURN 1.0;
END;
$$ LANGUAGE plpgsql;
```
### Запуск функции
```sql
SELECT check_materialized_view(
    'max_salary_position', 
    '[                      
        {"name": "position"},
        {"name": "max_salary"}
    ]'::JSONB,
    '[                      
        {"position": "Developer", "max_salary": 9500.00},
        {"position": "Manager", "max_salary": 12000.00}
    ]'::JSONB
);
```
## Ограничения целостности
### Задание
Проставьте такие ограничения таблицы employees, чтобы поле emp_id стало первичным ключом, поле emp_name не могло содержать пустых записей, а значения поля salary не превышали 500000.
### Решение
```sql
DROP TABLE IF EXISTS employees CASCADE;
CREATE TABLE employees (
	emp_id SERIAL,
	emp_name VARCHAR(100),
	position VARCHAR(50),
	salary DECIMAL(10, 2)
);
ALTER TABLE employees
ADD CONSTRAINT pk_emp_id PRIMARY KEY (emp_id);

ALTER TABLE employees
ALTER COLUMN emp_name SET NOT NULL;

ALTER TABLE employees
ADD CONSTRAINT chk_salary_max CHECK (salary <= 500000);
```
### Проверочная функция check_constraints()
На вход подаются:
название таблицы
описание ограничений в формате JSON (тип; имя столбца; для FK - имя связанной таблицы и колонки; для CHECK - текст проверки)

Функция вернёт:
по 1/n балла за каждое правильное ограничение, где n - кол-во ограничений
```sql
CREATE OR REPLACE FUNCTION check_constraints(
    p_table_name TEXT, 
    p_constraints_json JSON
) 
RETURNS NUMERIC AS
$$
DECLARE
    v_constraints JSON;
    v_constraint_count INT;
    v_score NUMERIC := 0.0;
    v_constraint JSON;
    v_constraint_type TEXT;
    v_column_name TEXT;
    v_constraint_text TEXT;
    v_check_result BOOLEAN;
    v_referenced_table TEXT;
    v_referenced_column TEXT;
BEGIN
    v_constraints := p_constraints_json;
    v_constraint_count := json_array_length(v_constraints);

    -- Проверка каждого ограничения
    FOR i IN 0..v_constraint_count - 1 LOOP
        v_constraint := v_constraints->i;
        v_constraint_type := v_constraint->>'type';
        v_column_name := v_constraint->>'column_name';
        v_constraint_text := v_constraint->>'constraint_text';

        CASE v_constraint_type
            WHEN 'primary_key' THEN
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_constraint pc
                    JOIN pg_class c ON c.oid = pc.conrelid
                    JOIN pg_attribute a ON a.attnum = ANY(pc.conkey) AND a.attrelid = c.oid
                    WHERE c.relname = p_table_name
                      AND pc.contype = 'p'
                      AND a.attname = v_column_name
                ) INTO v_check_result;

            WHEN 'not_null' THEN
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_attribute a
                    JOIN pg_class c ON c.oid = a.attrelid
                    WHERE c.relname = p_table_name
                      AND a.attname = v_column_name
                      AND a.attnotnull
                ) INTO v_check_result;

            WHEN 'unique' THEN
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_constraint pc
                    JOIN pg_class c ON c.oid = pc.conrelid
                    JOIN pg_attribute a ON a.attnum = ANY(pc.conkey) AND a.attrelid = c.oid
                    WHERE c.relname = p_table_name
                      AND pc.contype = 'u'
                      AND a.attname = v_column_name
                ) INTO v_check_result;

            WHEN 'foreign_key' THEN
                v_referenced_table := v_constraint->>'referenced_table';
                v_referenced_column := v_constraint->>'referenced_column';
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_constraint pc
                    JOIN pg_class c ON c.oid = pc.conrelid
                    JOIN pg_attribute a ON a.attnum = ANY(pc.conkey) AND a.attrelid = c.oid
                    WHERE c.relname = p_table_name
                      AND pc.contype = 'f'
                      AND a.attname = v_column_name
                      AND pc.confrelid = (SELECT oid FROM pg_class WHERE relname = v_referenced_table)
                      AND pc.confkey = ARRAY(SELECT attnum FROM pg_attribute 
                                             WHERE attrelid = pc.confrelid 
                                               AND attname = v_referenced_column)
                ) INTO v_check_result;

            WHEN 'check' THEN
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_constraint pc
                    JOIN pg_class c ON c.oid = pc.conrelid
                    WHERE c.relname = p_table_name
                      AND pc.contype = 'c'
                      AND pg_get_constraintdef(pc.oid) LIKE '%' || v_constraint_text || '%'
                ) INTO v_check_result;
        END CASE;

        IF v_check_result THEN
            v_score := v_score + (1.0 / v_constraint_count);
        END IF;
    END LOOP;

    RETURN ROUND(v_score, 1);
END;
$$ LANGUAGE plpgsql;
```
### Запуск функции
```sql
SELECT check_constraints(
    'employees', 
    '[
        {"type": "primary_key", "column_name": "emp_id"},
        {"type": "not_null", "column_name": "emp_name"},
        {"type": "check", "column_name": "salary", "constraint_text": "salary <= (500000)"}
    ]'
);
```
## Последовательности
### Задание
Создайте последовательность test_sequence, которая начинается с 10 и возрастает с шагом 5. 
### Решение
```sql
DROP SEQUENCE IF EXISTS test_sequence;
CREATE SEQUENCE test_sequence
    START WITH 10
    INCREMENT BY 5;
```
### Проверочная функция check_sequence()
На вход подаются:
название последовательности
начальное значение
шаг

Функция вернёт:
0.2 балла за существование последовательности с правильным названием
+ 0.3 балла, если один из параметров (начальное значение, шаг) правильный
+ 0.5 балла, если второй параметр правильный
Если последовательности не существует, сразу возвращается 0. 
```sql
CREATE OR REPLACE FUNCTION check_sequence(
	sequence_name TEXT,
	start_value BIGINT,
	increment_by BIGINT
) RETURNS NUMERIC AS $$
DECLARE
	is_sequence_exists BOOLEAN;
	actual_start_value BIGINT;
	actual_increment_by BIGINT;
BEGIN
    -- Проверка существования последовательности
    SELECT EXISTS (
    	SELECT 1
    	FROM pg_class c
    	JOIN pg_sequence s ON c.oid = s.seqrelid
    	WHERE c.relname = sequence_name
      	AND c.relkind = 'S' 	) INTO is_sequence_exists;
 
	IF NOT is_sequence_exists THEN
    	RETURN 0.0;
    END IF;
 
    -- Проверка параметров
    SELECT
    	s.seqstart AS start_value,
    	s.seqincrement AS increment_by
	INTO
    	actual_start_value,
    	actual_increment_by
	FROM pg_class c
	JOIN pg_sequence s ON c.oid = s.seqrelid
	WHERE c.relname = sequence_name;
 
	IF actual_start_value = start_value AND actual_increment_by = increment_by THEN
    	RETURN 1.0;
	ELSIF actual_start_value = start_value OR actual_increment_by = increment_by THEN
    	RETURN 0.5;
	ELSE
    	RETURN 0.2;
	END IF;
END;
$$ LANGUAGE plpgsql;
```
### Запуск функции
```sql
SELECT check_sequence('test_sequence', 10, 5);
```
## Триггеры
### Задание
Создайте триггер check_update,  который срабатывает перед обновлением таблицы employees и запускает функцию f_updated_emp() для каждой обновлённой записи.
### Решение
```sql
DROP TABLE IF EXISTS employees CASADE;
CREATE TABLE employees (
	emp_id SERIAL PRIMARY KEY,
	emp_name VARCHAR(100),
	position VARCHAR(50),
	salary DECIMAL(10, 2)
);
DROP TRIGGER IF EXISTS check_update ON employees;
CREATE OR REPLACE FUNCTION f_updated_emp() RETURNS TRIGGER AS $$
BEGIN
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER check_update
BEFORE UPDATE ON employees
FOR EACH ROW
EXECUTE FUNCTION f_updated_emp();
```
### Проверочная функция check_trigger()
На вход подаются:
название триггера
название таблицы
название вызываемой функции
ожидаемый тип триггера

Тип триггера - десятичное число, получаемое по битовой маске.
TRIGGER_TYPE_ROW				(1 << 0)
TRIGGER_TYPE_BEFORE				(1 << 1)
TRIGGER_TYPE_INSERT				(1 << 2)
TRIGGER_TYPE_DELETE				(1 << 3)
TRIGGER_TYPE_UPDATE				(1 << 4)
TRIGGER_TYPE_TRUNCATE			(1 << 5)
TRIGGER_TYPE_INSTEAD				(1 << 6)
Для триггера из примера  - UPDATE, BEFORE, ROW - битовая маска 0010011 - число 19.
Функция вернёт:
0.5 балла за существование триггера с правильным названием, таблицей и функцией
+ 0.5 балла, если тип триггера правильный
Если подходящий триггер не нашёлся, сразу возвращается 0.
```sql
CREATE OR REPLACE FUNCTION check_trigger(
    trigger_name TEXT, 
    table_name TEXT,  
    function_name TEXT,
    expected_trigger_type INT 
) RETURNS NUMERIC AS $$
DECLARE
    is_trigger_exists BOOLEAN;
    trigger_info RECORD;
BEGIN
    -- Проверка существования триггера
    SELECT EXISTS (
        SELECT 1
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_proc p ON t.tgfoid = p.oid
        WHERE t.tgname = trigger_name
        AND c.relname = table_name
        AND p.proname = function_name
    ) INTO is_trigger_exists;

    IF NOT is_trigger_exists THEN
        RETURN 0.0;
    END IF;

    -- Проверка параметров триггера
    SELECT
        t.tgname AS trigger_name,
        c.relname AS table_name,
        p.proname AS function_name,
        t.tgtype AS trigger_type
    INTO trigger_info
    FROM pg_trigger t
    JOIN pg_class c ON t.tgrelid = c.oid
    JOIN pg_proc p ON t.tgfoid = p.oid
    WHERE t.tgname = trigger_name
    AND c.relname = table_name
    AND p.proname = function_name;

    -- Проверка, что триггер имеет ожидаемый тип
    IF (trigger_info.trigger_type & expected_trigger_type) = 0 THEN
        RETURN 0.5;
    END IF;

    RETURN 1.0;
END;
$$ LANGUAGE plpgsql;
```
### Запуск функции
```sql
SELECT check_trigger('check_update', 'employees', 'f_updated_emp', 19);
```
## Индексы
### Задание
Создайте индекс на столбце emp_name в таблице employees.
### Решение
```sql
DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
	emp_id SERIAL PRIMARY KEY,
	emp_name VARCHAR(100),
	position VARCHAR(50),
	salary DECIMAL(10, 2)
);
DROP INDEX IF EXISTS idx_emp_name;
CREATE INDEX idx_emp_name ON employees (emp_name);
```
### Проверочная функция check_index()
На вход подаются:
название таблицы
название поля для индекса

Функция вернёт:
1 балл за верное решение
0 баллов, если индекс не найден 
```sql
CREATE OR REPLACE FUNCTION check_index(
    p_table_name TEXT,
    p_column_name TEXT
) RETURNS NUMERIC AS
$$
BEGIN
    RETURN (
        CASE WHEN EXISTS (
            SELECT 1
            FROM pg_index pi
            JOIN pg_class pc ON pc.oid = pi.indrelid
            JOIN pg_attribute pa ON pa.attnum = ANY(pi.indkey) AND pa.attrelid = pc.oid
            WHERE pc.relname = p_table_name
              AND pa.attname = p_column_name
        ) THEN 1.0
        ELSE 0.0
        END
    );
END;
$$ LANGUAGE plpgsql;
```
### Запуск функции
```sql
SELECT check_index('employees', 'emp_name');
```
