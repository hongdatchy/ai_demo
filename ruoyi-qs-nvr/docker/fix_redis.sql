USE `ry-config`;
UPDATE config_info SET content = REPLACE(content, 'host: localhost', 'host: ruoyi-redis') WHERE content LIKE '%host: localhost%';
UPDATE config_info SET content = REPLACE(content, 'host: 127.0.0.1', 'host: ruoyi-redis') WHERE content LIKE '%host: 127.0.0.1%';
UPDATE config_info SET content = REPLACE(content, 'jdbc:mysql://localhost:3306', 'jdbc:mysql://ruoyi-mysql:3306') WHERE content LIKE '%jdbc:mysql://localhost:3306%';
UPDATE config_info SET content = REPLACE(content, 'jdbc:mysql://127.0.0.1:3306', 'jdbc:mysql://ruoyi-mysql:3306') WHERE content LIKE '%jdbc:mysql://127.0.0.1:3306%';
UPDATE config_info SET content = REPLACE(content, 'password: haoxin', 'password: password') WHERE content LIKE '%password: haoxin%';
SELECT data_id, SUBSTRING(content, 1, 300) FROM config_info WHERE data_id IN ('ruoyi-auth-dev.yml','application-dev.yml','ruoyi-gateway-dev.yml');
