-- public has usage and create by default. revoke it.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE USAGE ON SCHEMA public FROM PUBLIC;

--this assumes a role "sqlgReadOnly" has been created.
GRANT USAGE ON ALL TABLES IN SCHEMA public TO "sqlgReadOnly";
GRANT USAGE ON ALL TABLES IN SCHEMA sqlg_schema TO "sqlgReadOnly";

GRANT SELECT ON ALL TABLES IN SCHEMA sqlg_schema TO "sqlgReadOnly";

SELECT *, grantee, privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'sqlgReadOnly'