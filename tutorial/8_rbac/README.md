# Tutorial 8: Role-based access control (RBAC)

Demonstrates access management with `MilvusClientV2` using an administrator credential: create a
user, a role, and a privilege group, add built-in privileges to the group, attach the group to the
role, and grant the role to the user. The tutorial then inspects the assignments and cleans
everything up.

## What you learn

1. Create a login identity with `createUser`.
2. Create a permission container with `createRole`.
3. Create a reusable privilege group with `createPrivilegeGroup`.
4. Add built-in privileges such as `Query` and `Search` to a group.
5. Attach a privilege group to a role with `grantPrivilegeV2`.
6. Assign a role to a user with `grantRole`.
7. Inspect assignments with `describeUser` and `describeRole`.
8. Revoke and delete every created resource during cleanup.

## Prerequisites

- Java 8 or higher
- Apache Maven
- Milvus 2.6 or later running at an accessible endpoint
- An administrator credential: the `MILVUS_TOKEN` must be allowed to create users, roles,
  privilege groups, and grants

The tutorial defaults to `MILVUS_URI=http://localhost:19530` and `MILVUS_TOKEN=root:Milvus`.
Override them when needed:

```bash
MILVUS_URI="https://your-endpoint" MILVUS_TOKEN="your-token" \
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.rbac.App"
```

## Build and run

From this directory:

```bash
mvn compile
mvn exec:java -Dexec.mainClass="io.milvus.tutorial.rbac.App"
```
