# db-model-generator

## Overview

`db-model-generator` is a tool that automatically generates Go models and methods for interacting with PostgreSQL databases based on SQL table definitions. It scans `.sql` schema files and generates Go code with essential database operations using the pgx/v5 driver.

### Supported Operations

For each PostgreSQL table, the following methods are generated:

- `Create`: Insert a new record into the table.
- `CreateMut`: Batch insert records into the table.
- `CreateOrUpdateMut`: Insert a new record or update an existing one.
- `Find`: Fetch a row by the primary key.
- `FindRtx`: Fetch a row by the primary key using a read-only transaction.
- `Exists`: Check if a row exists by the primary key.
- `ExistsRtx`: Check if a row exists by the primary key using a read-only transaction.
- `Get`: Retrieve a row with detailed information based on query parameters.
- `GetRtx`: Retrieve a row with detailed information based on query parameters using a read-only transaction.
- `Update`: Update an existing record in the table.
- `UpdateMut`: Batch update records in the table.
- `Delete`: Delete a record from the table.
- `DeleteMut`: Batch delete records from the table.
- `GetIter`: Retrieve multiple rows with detailed information based on query parameters and iterate over them using a callback.
- `GetRtxIter`: Retrieve multiple rows with detailed information based on query parameters using a read-only transaction and iterate over them using a callback.
- `Retrieve`: Fetch a row by primary keys and return all fields.
- `RetrieveRtx`: Fetch a row by primary keys using a read-only transaction and return all fields.
- `GetByPrimaryKeys`: Fetch multiple rows by primary keys.
- `ListByPrimaryKeys`: List multiple rows by primary keys.
- `GetByPrimaryKeysRtx`: Fetch multiple rows by primary keys using a read-only transaction.
- `ListByPrimaryKeysRtx`: List multiple rows by primary keys using a read-only transaction.

### Flags

- `-c`: Create SQL files mode.
- `-f`: Specific file mode, allowing the user to specify a certain file.
- `-id`: Basic ID mode, which requires a valid basic ID string.

### Features

- **Automatic Code Generation**: Reads `.sql` files to generate Go models with essential operations for PostgreSQL.
- **CRUD + Mutations**: Supports basic CRUD operations and batch mutations.
- **PostgreSQL Native**: Uses pgx/v5 driver for optimal PostgreSQL performance.
- **Type Support**: Full support for PostgreSQL types including UUID, JSONB, arrays, and custom types.
- **Customizable**: Easily extendable to support additional methods.
- **One SQL file per folder**: Each folder should contain only one SQL file. Having multiple files may cause errors.
- **If you add the `-c` flag**: You can place a .sql file in the root of the project, and the files will be created automatically.



### Basic ID Mode (`-id`)
Example:  `db-model-generator -id company_id` 


When running the tool with the `-id` flag, the generated code will include `Get` methods with the basic ID in method parameters. This mode is particularly useful when working with tables that have a common identifier field (e.g., `company_id`, `organization_id`).

### Important Notes
- You cannot use both create SQL files mode and specific file mode simultaneously.
- The specified file must be a `.sql` file, not a directory.

### ⚠️ Warning  
Using the `-c` flag can **overwrite old files**. Ensure you have a backup or use version control to prevent accidental data loss.

## Installation

You can install `db-model-generator` globally using `go install`:

```bash
go install github.com/Zumu-AI/db-model-generator@latest