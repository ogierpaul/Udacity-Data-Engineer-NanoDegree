-- Create Read-Only Group

CREATE GROUP ro_group;

-- Create User

CREATE USER {ro_user_name} WITH password {ro_user_password};

-- Add User to Read-Only Group

ALTER GROUP ro_group ADD USER {ro_user_name};

-- Grant Usage permission to Read-Only Group to specific Schema

GRANT USAGE ON SCHEMA {schemaout} TO GROUP ro_group;

-- Grant Select permission to Read-Only Group to specific Schema

GRANT SELECT ON ALL TABLES IN SCHEMA {schemaout} TO GROUP ro_group;

-- Alter Default Privileges to maintain the permissions on new tables

ALTER DEFAULT PRIVILEGES IN SCHEMA {schemaout} GRANT SELECT ON TABLES TO GROUP ro_group;

-- Revoke CREATE privileges from group

REVOKE CREATE ON SCHEMA {schemaout} FROM GROUP ro_group;