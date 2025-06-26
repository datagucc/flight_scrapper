CREATE ROLE powerbi_user WITH LOGIN PASSWORD 'pwb_aq278_po';
GRANT CONNECT ON DATABASE idle_city TO powerbi_user;
GRANT USAGE ON SCHEMA public,"FPT","weather" TO powerbi_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public,"FPT",weather TO powerbi_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public ,"FPT","weather" GRANT SELECT ON TABLES TO powerbi_user;


show password_encryption;