# Database Migration Scripts

This directory contains database migration scripts for the HWC Potree API.

## Available Migrations

### migrate_add_cancelled_field.py

**Purpose:** Adds the `cancelled` boolean field to all existing Job documents in the database.

**When to run:** This migration should be run once after deploying the job cancellation feature to ensure all existing jobs have the `cancelled` field.

**Usage:**

```bash
python bin/migrate_add_cancelled_field.py
```

**What it does:**

1. Connects to the MongoDB database using environment variables
2. Finds all Job documents that don't have a `cancelled` field
3. Adds `cancelled: False` to those documents
4. Updates the `updated_at` timestamp
5. Verifies the migration completed successfully

**Safety:**

- The script prompts for confirmation before running
- Only updates documents that don't already have the `cancelled` field (idempotent)
- Does not modify any other fields
- Provides detailed output about the migration progress

**Requirements:**

- `.env` file must be configured with `MONGO_CONNECTION_STRING` and `NAME`
- Python dependencies must be installed (`pymongo`, `python-dotenv`)

**Example output:**

```
============================================================
Job Cancellation Field Migration
============================================================

This will add 'cancelled' field to all existing jobs. Continue? (y/n): y

Connecting to MongoDB database: hwc-potree
Found 15 jobs without 'cancelled' field
Migration completed successfully!
  - Matched documents: 15
  - Modified documents: 15
Verification passed - all jobs now have 'cancelled' field
Database connection closed
============================================================
```

## Running Migrations

1. Ensure your `.env` file is properly configured
2. Make sure you have a backup of your database (recommended)
3. Run the migration script
4. Verify the migration completed successfully
5. Test the application to ensure everything works as expected

## Rollback

If you need to rollback the `cancelled` field migration, you can use the MongoDB shell:

```javascript
db.Job.updateMany(
  { cancelled: { $exists: true } },
  { $unset: { cancelled: "" } }
);
```

**Note:** Only rollback if absolutely necessary, as the application code expects this field to exist.
