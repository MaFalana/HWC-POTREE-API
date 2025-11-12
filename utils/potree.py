import os
import subprocess
import logging

from models.Project import Project
from config.main import DB

AZ = DB.az

logger = logging.getLogger(__name__)


class PotreeConverter:
    def __init__(self):
        self.path = os.getenv("POTREE_PATH", "/app/PotreeConverter")
        logger.info(f"PotreeConverter initialized with path: {self.path}")

    def convert(self, input_path: str, output_dir: str, project: Project) -> str:
        """
        Convert LAS/LAZ file to Potree format.
        
        Args:
            input_path: Path to input LAS/LAZ file
            output_dir: Directory where Potree output will be saved
            project: Project model with metadata
            
        Returns:
            Path to the output directory
            
        Raises:
            subprocess.CalledProcessError: If conversion fails
            FileNotFoundError: If PotreeConverter binary not found
        """
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Converting {input_path} to Potree format in {output_dir}")

        # Build PotreeConverter command
        args = [
            self.path,
            input_path,
            "-o", output_dir,
            "--overwrite"
        ]
        
        # Add projection (CRS proj4 is now required)
        args.extend(["--projection", project.crs.proj4])
        logger.info(f"Using CRS proj4: {project.crs.proj4}")

        logger.info(f"Running PotreeConverter with args: {' '.join(args)}")

        try:
            # Get the directory containing PotreeConverter binary
            potree_dir = os.path.dirname(self.path)
            
            # Run PotreeConverter from its directory (it needs access to resources/)
            proc = subprocess.Popen(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=potree_dir
            )

            # Stream and log output
            output_lines = []
            while proc.poll() is None:
                line = proc.stdout.readline()
                if line:
                    output_lines.append(line.rstrip())
                    logger.info(f"PotreeConverter: {line.rstrip()}")

            # Get any remaining output
            remaining = proc.stdout.read()
            if remaining:
                for line in remaining.split('\n'):
                    if line:
                        output_lines.append(line.rstrip())
                        logger.info(f"PotreeConverter: {line.rstrip()}")

            # Check return code
            if proc.returncode != 0:
                error_msg = f"PotreeConverter failed with return code {proc.returncode}"
                logger.error(error_msg)
                logger.error(f"Output: {chr(10).join(output_lines)}")
                raise subprocess.CalledProcessError(
                    proc.returncode,
                    args,
                    output='\n'.join(output_lines)
                )

            logger.info(f"PotreeConverter completed successfully. Output in {output_dir}")
            return output_dir

        except FileNotFoundError as e:
            logger.error(f"PotreeConverter binary not found at {self.path}")
            raise FileNotFoundError(f"PotreeConverter binary not found at {self.path}") from e
        except Exception as e:
            logger.error(f"Error during Potree conversion: {e}", exc_info=True)
            raise

    def upload_output(self, output_dir: str, project_id: str) -> str:
        """
        Upload all Potree output files to Azure Blob Storage and return metadata.json SAS URL.
        
        Potree Converter produces:
        - metadata.json (main metadata file)
        - hierarchy.bin (octree structure)
        - r/ folder (octree data files at different LOD levels)
        - No HTML files are generated (viewer is handled separately)
        
        Args:
            output_dir: Local directory containing Potree output files
            project_id: Project ID to use as blob prefix
            
        Returns:
            SAS URL for the metadata.json file
            
        Raises:
            FileNotFoundError: If output directory or metadata.json doesn't exist
            Exception: If upload fails
        """
        if not os.path.exists(output_dir):
            raise FileNotFoundError(f"Output directory not found: {output_dir}")
        
        logger.info(f"Uploading Potree output from {output_dir} to Azure with prefix {project_id}/")
        
        try:
            # Upload entire folder with project_id as prefix
            # This maintains the folder structure and sets correct MIME types
            blob_prefix = f"{project_id}/"
            AZ.upload_folder(output_dir, blob_prefix)
            
            logger.info(f"Successfully uploaded Potree files for project {project_id}")
            
            # Potree produces metadata.json as the main metadata file
            metadata_path = os.path.join(output_dir, "metadata.json")
            if not os.path.exists(metadata_path):
                logger.error(f"metadata.json not found at {metadata_path}")
                raise FileNotFoundError(f"metadata.json not found in Potree output at {metadata_path}")
            
            # Generate SAS URL for metadata.json
            metadata_blob = f"{project_id}/metadata.json"
            sas_url = AZ.generate_sas_url(metadata_blob)
            logger.info(f"Generated SAS URL for metadata.json: {metadata_blob}")
            
            return sas_url
                
        except Exception as e:
            logger.error(f"Failed to upload Potree output: {e}", exc_info=True)
            raise
