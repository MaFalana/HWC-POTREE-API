#!/usr/bin/env python3
"""
Database migration script to add 'cancelled' field to existing Job documents.

This script adds the 'cancelled' boolean field (default: False) to all existing
jobs in the database that don't already have this field.

Usage:
    python bin/migrate_add_cancelled_field.py
"""

import os
import sys
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

# Add parent directory to path to import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Load environment variables
load_dotenv()


def migrate_add_cancelled_field():
    """
    Add 'cancelled' field to all existing Job documents that don't have it.
    """
    # Connect to MongoDB
    name = os.getenv("NAME")
    conn = os.getenv("MONGO_CONNECTION_STRING")
    
    if not conn or not name:
        print("Error: MONGO_CONNECTION_STRING or NAME environment variable not set")
        sys.exit(1)
    
    print(f"Connecting to MongoDB database: {name}")
    client = MongoClient(conn)
    db = client[name]
    jobs_collection = db['Job']
    
    try:
        # Find all jobs that don't have the 'cancelled' field
        jobs_without_cancelled = jobs_collection.count_documents({
            'cancelled': {'$exists': False}
        })
        
        print(f"Found {jobs_without_cancelled} jobs without 'cancelled' field")
        
        if jobs_without_cancelled == 0:
            print("No migration needed - all jobs already have 'cancelled' field")
            return
        
        # Update all jobs without the 'cancelled' field
        result = jobs_collection.update_many(
            {'cancelled': {'$exists': False}},
            {
                '$set': {
                    'cancelled': False,
                    'updated_at': datetime.utcnow()
                }
            }
        )
        
        print(f"Migration completed successfully!")
        print(f"  - Matched documents: {result.matched_count}")
        print(f"  - Modified documents: {result.modified_count}")
        
        # Verify the migration
        remaining = jobs_collection.count_documents({
            'cancelled': {'$exists': False}
        })
        
        if remaining > 0:
            print(f"Warning: {remaining} jobs still don't have 'cancelled' field")
        else:
            print("Verification passed - all jobs now have 'cancelled' field")
            
    except Exception as e:
        print(f"Error during migration: {e}")
        sys.exit(1)
    finally:
        client.close()
        print("Database connection closed")


if __name__ == "__main__":
    print("=" * 60)
    print("Job Cancellation Field Migration")
    print("=" * 60)
    print()
    
    # Confirm before running
    response = input("This will add 'cancelled' field to all existing jobs. Continue? (y/n): ")
    if response.lower() != 'y':
        print("Migration cancelled by user")
        sys.exit(0)
    
    print()
    migrate_add_cancelled_field()
    print()
    print("=" * 60)
