#!/usr/bin/env python3
"""
Migration script to convert existing ortho PNGs to WebP format.

This script:
1. Finds all projects with ortho.url ending in .png
2. Downloads PNG from Azure
3. Converts to WebP using gdal_translate
4. Uploads WebP to Azure
5. Updates database with new URL
6. Optionally deletes old PNG

Usage:
    python scripts/migrate_ortho_to_webp.py [--dry-run] [--delete-old]

Options:
    --dry-run: Show what would be migrated without making changes
    --delete-old: Delete old PNG files from Azure after successful migration
"""

import os
import sys
import argparse
import tempfile
import subprocess
import logging
from pathlib import Path
from typing import List, Dict, Optional

# Add parent directory to path to import project modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from storage.db import DatabaseManager
from models.Project import Project

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OrthoMigrator:
    """Migrates ortho PNGs to WebP format."""
    
    def __init__(self, db: DatabaseManager, dry_run: bool = False, delete_old: bool = False):
        """
        Initialize the migrator.
        
        Args:
            db: DatabaseManager instance
            dry_run: If True, only show what would be migrated
            delete_old: If True, delete old PNG files after migration
        """
        self.db = db
        self.dry_run = dry_run
        self.delete_old = delete_old
        self.stats = {
            'total': 0,
            'migrated': 0,
            'failed': 0,
            'skipped': 0
        }
        self.successful_projects = []
        self.failed_projects = []
    
    def find_projects_with_png_orthos(self) -> List[Project]:
        """
        Find all projects with ortho URLs ending in .png or .png?...
        
        Returns:
            List of Project objects with PNG orthos
        """
        logger.info("Searching for projects with PNG orthos...")
        
        # Query projects where ortho.url contains .png
        query = {
            'ortho.url': {'$regex': r'\.png', '$options': 'i'}
        }
        
        projects = []
        cursor = self.db.projectsCollection.find(query)
        
        for doc in cursor:
            try:
                project = Project(**doc)
                if project.ortho and project.ortho.url:
                    # Check if URL actually ends with .png (ignoring query params)
                    url_path = project.ortho.url.split('?')[0]
                    if url_path.endswith('.png'):
                        projects.append(project)
                        logger.info(f"Found project {project.id} with PNG ortho")
            except Exception as e:
                logger.error(f"Error parsing project {doc.get('_id')}: {e}")
        
        logger.info(f"Found {len(projects)} projects with PNG orthos")
        return projects
    
    def convert_png_to_webp(self, png_path: str, webp_path: str) -> bool:
        """
        Convert PNG to WebP using gdal_translate.
        
        Automatically downsamples if image exceeds WebP's 16383x16383 limit.
        
        Args:
            png_path: Path to input PNG file
            webp_path: Path to output WebP file
            
        Returns:
            True if conversion succeeded, False otherwise
        """
        try:
            logger.info(f"Converting {png_path} to WebP...")
            
            # First, check image dimensions
            info_result = subprocess.run([
                'gdalinfo', '-json', png_path
            ], capture_output=True, text=True, timeout=30)
            
            if info_result.returncode != 0:
                logger.error(f"Failed to get image info: {info_result.stderr}")
                return False
            
            import json
            info = json.loads(info_result.stdout)
            width = info.get('size', [0, 0])[0]
            height = info.get('size', [0, 0])[1]
            
            logger.info(f"Image dimensions: {width}x{height}")
            
            # WebP maximum dimensions are 16383x16383
            MAX_WEBP_DIM = 16383
            
            # Build conversion command
            cmd = [
                'gdal_translate',
                '-of', 'WEBP',
                '-co', 'QUALITY=90',
                '-co', 'LOSSLESS=NO',
            ]
            
            # Check if we need to downsample
            if width > MAX_WEBP_DIM or height > MAX_WEBP_DIM:
                # Calculate scale factor to fit within WebP limits
                scale = min(MAX_WEBP_DIM / width, MAX_WEBP_DIM / height)
                scale_percent = int(scale * 100)
                
                logger.warning(f"Image exceeds WebP limits ({MAX_WEBP_DIM}x{MAX_WEBP_DIM})")
                logger.info(f"Downsampling to {scale_percent}% to fit within WebP limits")
                
                cmd.extend(['-outsize', f'{scale_percent}%', '0'])
            
            cmd.extend([png_path, webp_path])
            
            # Run conversion
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode != 0:
                logger.error(f"Conversion failed: {result.stderr}")
                return False
            
            if not os.path.exists(webp_path):
                logger.error(f"WebP file not created at {webp_path}")
                return False
            
            # Log file size comparison
            png_size = os.path.getsize(png_path) / (1024 * 1024)
            webp_size = os.path.getsize(webp_path) / (1024 * 1024)
            savings = ((png_size - webp_size) / png_size) * 100
            
            logger.info(f"Conversion successful: {png_size:.2f}MB → {webp_size:.2f}MB ({savings:.1f}% smaller)")
            return True
            
        except subprocess.TimeoutExpired:
            logger.error("Conversion timed out after 5 minutes")
            return False
        except FileNotFoundError:
            logger.error("gdal_translate not found - GDAL may not be installed")
            return False
        except Exception as e:
            logger.error(f"Error during conversion: {e}")
            return False
    
    def migrate_project_ortho(self, project: Project) -> bool:
        """
        Migrate a single project's ortho from PNG to WebP.
        
        Args:
            project: Project object with PNG ortho
            
        Returns:
            True if migration succeeded, False otherwise
        """
        logger.info(f"Migrating project {project.id}...")
        
        if not project.ortho or not project.ortho.url:
            logger.warning(f"Project {project.id} has no ortho URL, skipping")
            return False
        
        # Extract blob name from URL
        # URL format: https://storage.blob.core.windows.net/container/path/ortho.png?sas
        url_parts = project.ortho.url.split('?')[0]  # Remove SAS token
        blob_name = url_parts.split(f"{self.db.az.container_name}/")[-1]
        
        if not blob_name.endswith('.png'):
            logger.warning(f"Project {project.id} ortho URL doesn't end with .png: {blob_name}")
            return False
        
        # Create temp directory for this migration
        temp_dir = tempfile.mkdtemp(prefix=f"ortho_migrate_{project.id}_")
        png_path = os.path.join(temp_dir, 'ortho.png')
        webp_path = os.path.join(temp_dir, 'ortho.webp')
        
        try:
            # Step 1: Download PNG from Azure
            logger.info(f"Downloading PNG from Azure: {blob_name}")
            self.db.az.download_file(blob_name, png_path)
            
            if not os.path.exists(png_path):
                logger.error(f"Failed to download PNG for project {project.id}")
                return False
            
            # Step 2: Convert to WebP
            if not self.convert_png_to_webp(png_path, webp_path):
                logger.error(f"Failed to convert PNG to WebP for project {project.id}")
                return False
            
            if self.dry_run:
                logger.info(f"[DRY RUN] Would upload WebP and update database for project {project.id}")
                return True
            
            # Step 3: Upload WebP to Azure
            webp_blob_name = blob_name.replace('.png', '.webp')
            logger.info(f"Uploading WebP to Azure: {webp_blob_name}")
            
            with open(webp_path, 'rb') as f:
                self.db.az.upload_bytes(
                    data=f.read(),
                    blob_name=webp_blob_name,
                    content_type="image/webp",
                    overwrite=True
                )
            
            # Step 4: Update database with new URL
            new_url = self.db.az.get_public_url(webp_blob_name)
            project.ortho.url = new_url
            
            # Also migrate thumbnail if it exists and is PNG
            if project.ortho.thumbnail and '.png' in project.ortho.thumbnail:
                thumbnail_blob = project.ortho.thumbnail.split('?')[0].split(f"{self.db.az.container_name}/")[-1]
                if thumbnail_blob.endswith('.png'):
                    logger.info(f"Migrating thumbnail for project {project.id}")
                    
                    thumb_png_path = os.path.join(temp_dir, 'thumb.png')
                    thumb_webp_path = os.path.join(temp_dir, 'thumb.webp')
                    
                    try:
                        # Download thumbnail PNG
                        self.db.az.download_file(thumbnail_blob, thumb_png_path)
                        
                        # Convert to WebP
                        if self.convert_png_to_webp(thumb_png_path, thumb_webp_path):
                            # Upload WebP thumbnail
                            thumb_webp_blob = thumbnail_blob.replace('.png', '.webp')
                            with open(thumb_webp_path, 'rb') as f:
                                self.db.az.upload_bytes(
                                    data=f.read(),
                                    blob_name=thumb_webp_blob,
                                    content_type="image/webp",
                                    overwrite=True
                                )
                            
                            # Update thumbnail URL
                            project.ortho.thumbnail = self.db.az.get_public_url(thumb_webp_blob)
                            logger.info(f"Thumbnail migrated successfully")
                            
                            # Delete old thumbnail PNG if requested
                            if self.delete_old:
                                try:
                                    self.db.az.delete_blob(thumbnail_blob)
                                    logger.info(f"Deleted old thumbnail PNG: {thumbnail_blob}")
                                except Exception as e:
                                    logger.warning(f"Failed to delete old thumbnail PNG: {e}")
                    except Exception as e:
                        logger.warning(f"Failed to migrate thumbnail: {e}")
            
            # Update project in database
            self.db.updateProject(project)
            logger.info(f"Database updated with new WebP URL for project {project.id}")
            
            # Step 5: Delete old PNG if requested
            if self.delete_old:
                try:
                    self.db.az.delete_blob(blob_name)
                    logger.info(f"Deleted old PNG: {blob_name}")
                except Exception as e:
                    logger.warning(f"Failed to delete old PNG: {e}")
            
            logger.info(f"Successfully migrated project {project.id}")
            return True
            
        except Exception as e:
            logger.error(f"Error migrating project {project.id}: {e}", exc_info=True)
            return False
        finally:
            # Cleanup temp directory
            try:
                import shutil
                shutil.rmtree(temp_dir)
            except Exception as e:
                logger.warning(f"Failed to cleanup temp directory: {e}")
    
    def migrate_all(self) -> Dict[str, int]:
        """
        Migrate all projects with PNG orthos to WebP.
        
        Returns:
            Dictionary with migration statistics
        """
        logger.info("Starting ortho PNG to WebP migration...")
        
        if self.dry_run:
            logger.info("DRY RUN MODE - No changes will be made")
        
        # Find projects with PNG orthos
        projects = self.find_projects_with_png_orthos()
        self.stats['total'] = len(projects)
        
        if self.stats['total'] == 0:
            logger.info("No projects with PNG orthos found")
            return self.stats
        
        # Migrate each project
        for i, project in enumerate(projects, 1):
            logger.info(f"Processing project {i}/{self.stats['total']}: {project.id}")
            
            try:
                if self.migrate_project_ortho(project):
                    self.stats['migrated'] += 1
                    self.successful_projects.append(project.id)
                else:
                    self.stats['failed'] += 1
                    self.failed_projects.append(project.id)
            except Exception as e:
                logger.error(f"Unexpected error migrating project {project.id}: {e}")
                self.stats['failed'] += 1
                self.failed_projects.append(project.id)
        
        # Print summary
        logger.info("=" * 60)
        logger.info("Migration Summary:")
        logger.info(f"  Total projects found: {self.stats['total']}")
        logger.info(f"  Successfully migrated: {self.stats['migrated']}")
        logger.info(f"  Failed: {self.stats['failed']}")
        logger.info(f"  Skipped: {self.stats['skipped']}")
        logger.info("=" * 60)
        
        # Print successful projects
        if self.successful_projects:
            logger.info("\nSuccessfully migrated projects:")
            for project_id in self.successful_projects:
                logger.info(f"  ✓ {project_id}")
        
        # Print failed projects
        if self.failed_projects:
            logger.info("\nFailed migrations (need manual review):")
            for project_id in self.failed_projects:
                logger.info(f"  ✗ {project_id}")
            logger.info("\nTo retry failed projects, fix the issues and run the script again.")
            logger.info("The script will skip already-migrated projects (those with .webp URLs).")
        
        logger.info("=" * 60)
        
        return self.stats


def main():
    """Main entry point for the migration script."""
    parser = argparse.ArgumentParser(
        description='Migrate ortho PNGs to WebP format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run to see what would be migrated
  python scripts/migrate_ortho_to_webp.py --dry-run
  
  # Migrate all orthos (keep old PNGs)
  python scripts/migrate_ortho_to_webp.py
  
  # Migrate and delete old PNGs
  python scripts/migrate_ortho_to_webp.py --delete-old
        """
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be migrated without making changes'
    )
    
    parser.add_argument(
        '--delete-old',
        action='store_true',
        help='Delete old PNG files from Azure after successful migration'
    )
    
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    # Check for required environment variables
    required_vars = [
        'MONGO_CONNECTION_STRING',
        'AZURE_STORAGE_CONNECTION_STRING'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)
    
    # Initialize database manager
    try:
        db = DatabaseManager()
        logger.info("Connected to database successfully")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        sys.exit(1)
    
    # Run migration
    migrator = OrthoMigrator(db, dry_run=args.dry_run, delete_old=args.delete_old)
    
    try:
        stats = migrator.migrate_all()
        
        # Exit with error code if any migrations failed
        if stats['failed'] > 0:
            sys.exit(1)
        
    except KeyboardInterrupt:
        logger.info("\nMigration interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
