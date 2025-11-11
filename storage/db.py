import os
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import List, Optional

from models.Project import Project
from models.Job import Job

from storage.az import AzureStorageManager
from io import BytesIO

#load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.name = os.getenv("NAME") # Name of the database collection and container
        self.az = AzureStorageManager(self.name) # Initialize Azure Storage Manager
        conn = os.getenv("MONGO_CONNECTION_STRING")
        self.client = MongoClient(conn)
        self.db = self.client[self.name]
        print(f'Connected to MongoDB database: {self.name}\n') 
        self.projectsCollection = self.db['Project'] # Get the Project collection from the database
        self.jobsCollection = self.db['Job'] # Get the Job collection from the database
        
        # Ensure indexes exist for efficient queries
        self._ensure_indexes()

    def query(self, query):
        #collection = self.db[collection_name]
        return list(self.projectsCollection.find(query))

    def insert(self, document):
        #collection = self.db[collection_name]
        self.projectsCollection.insert_one(document)
        #result = collection.insert_one(document)
        #return result.inserted_id

    def close(self):
        self.client.close()
    
    def _ensure_indexes(self):
        """
        Ensure required indexes exist on collections.
        This is especially important for Azure Cosmos DB which requires
        explicit indexes for sort operations.
        """
        try:
            # Create index on jobs collection for created_at (used for FIFO sorting)
            self.jobsCollection.create_index([("created_at", 1)], background=True)
            print("Ensured index on jobs.created_at")
            
            # Create index on jobs collection for status (used for filtering pending jobs)
            self.jobsCollection.create_index([("status", 1)], background=True)
            print("Ensured index on jobs.status")
            
            # Create compound index for efficient job queries (status + created_at)
            self.jobsCollection.create_index([("status", 1), ("created_at", 1)], background=True)
            print("Ensured compound index on jobs.status+created_at")
            
            # Create index on jobs collection for project_id (used for getting jobs by project)
            self.jobsCollection.create_index([("project_id", 1)], background=True)
            print("Ensured index on jobs.project_id")
            
        except Exception as e:
            print(f"Warning: Failed to create indexes: {e}")
            # Don't fail initialization if index creation fails
            pass
        

    async def addProject(self, project: Project): # Creates a new project in the database - CREATE (Mongo Only)

        if self.exists('Project', {'_id': project.id}):  # Check if project already exists
            print(f"Project with id {project.id} already exists. Skipping insertion.")
            return

        else:
            print(f"Project with id {project.id} does not exist. Adding new project.")
            doc = project._to_dict()
            self.projectsCollection.insert_one(doc)  # If it doesn't, add the project
            print(f"Added project: {project} with _id {project.id}")

    def getProjects(self, query): # Gets projects from the database - READ
        projects = self.query(query)
        print(f"Found projects: {projects}")
        return projects


    def getProjectsList(self, payload: list):
        projects = []
        for id in payload:
            project = self.getProject({'_id': id})
            if project:
                projects.append(project)
        print(f"Found projects: {projects}")
        return projects



    def getProject(self, query) -> Project:

        results = self.query(query)
        if not results:
            return None
        project = Project(**results[0])
        return project  # Return a single project



    def updateProject(self, project: Project): # Updates a project in the database - UPDATE
        doc = project._to_dict()
        self.projectsCollection.update_one({'_id': project.id}, {'$set': doc})
        print(f"Updated project: {project.name} with id {project.id}")


    # def updateProject(self, project_id: str, description=None, tags=None, append=False):
    #     update_fields = {}
    #     if description is not None:
    #         update_fields["description"] = description
    #     if tags is not None:
    #         if append:
    #             self.projectsCollection.update_one(
    #                 {"_id": project_id},
    #                 {"$addToSet": {"tags": {"$each": tags}}}
    #             )
    #             return
    #         update_fields["tags"] = tags

    #     if update_fields:
    #         self.projectsCollection.update_one({"_id": project_id}, {"$set": update_fields})



    def deleteProject(self, id):  # id is a string
        """Deletes both the Mongo document and the Azure blob."""
        # find the document
        project = self.getProject({'_id': id})
        if not project:
            print(f"Project with id {id} not found")
            return False

        # delete from Mongo
        self.projectsCollection.delete_one({'_id': id})
        print(f"Deleted MongoDB record for {id}")

        # delete from Azure
        try:
            self.az.delete_blob(id)
            print(f"Deleted Azure blob for {id}")
        except Exception as e:
            print(f"Azure delete failed for {id}: {e}")

        # Get project name safely
        project_name = project.name if hasattr(project, 'name') else id
        print(f"Deleted project: {project_name} with id {id}")
        return True





    def exists(self, collection_name, query): # Checks if a document exists in the database, return boolean
        collection = self.db[collection_name]
        return collection.find_one(query) != None


    # Job Management Methods

    def create_job(self, project_id: str, file_path: str, azure_path: str, job_id: str) -> str:
        """
        Create a new job record in the database
        
        Args:
            project_id: The project ID this job belongs to
            file_path: Local temporary file path
            azure_path: Azure blob path (jobs/{job_id}.laz)
            job_id: Unique job identifier (UUID)
            
        Returns:
            job_id: The created job ID
            
        Raises:
            ValueError: If a job with the same ID already exists
        """
        # Check if job already exists
        existing_job = self.jobsCollection.find_one({'_id': job_id})
        if existing_job:
            raise ValueError(f"Job with id {job_id} already exists")
        
        job = Job(
            id=job_id,
            project_id=project_id,
            status="pending",
            file_path=file_path,
            azure_path=azure_path,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        doc = job._to_dict()
        self.jobsCollection.insert_one(doc)
        print(f"Created job {job_id} for project {project_id}")
        return job_id


    def get_job(self, job_id: str) -> Optional[Job]:
        """
        Get a job by its ID
        
        Args:
            job_id: The job ID to retrieve
            
        Returns:
            Job object if found, None otherwise
        """
        result = self.jobsCollection.find_one({'_id': job_id})
        if not result:
            return None
        return Job(**result)


    def update_job_status(self, job_id: str, status: str, **kwargs):
        """
        Update job status and other fields
        
        Args:
            job_id: The job ID to update
            status: New status (pending, processing, completed, failed)
            **kwargs: Additional fields to update (current_step, progress_message, error_message, etc.)
        """
        update_fields = {
            'status': status,
            'updated_at': datetime.utcnow()
        }
        
        # Add optional fields if provided
        if 'current_step' in kwargs:
            update_fields['current_step'] = kwargs['current_step']
        if 'progress_message' in kwargs:
            update_fields['progress_message'] = kwargs['progress_message']
        if 'error_message' in kwargs:
            update_fields['error_message'] = kwargs['error_message']
        if 'retry_count' in kwargs:
            update_fields['retry_count'] = kwargs['retry_count']
        
        # Set completed_at timestamp if status is completed or failed
        if status in ['completed', 'failed']:
            update_fields['completed_at'] = datetime.utcnow()
        
        self.jobsCollection.update_one(
            {'_id': job_id},
            {'$set': update_fields}
        )
        print(f"Updated job {job_id} status to {status}")


    def get_jobs_by_project(self, project_id: str) -> List[Job]:
        """
        Get all jobs for a specific project
        
        Args:
            project_id: The project ID to filter by
            
        Returns:
            List of Job objects
        """
        results = list(self.jobsCollection.find({'project_id': project_id}).sort('created_at', -1))
        jobs = [Job(**result) for result in results]
        print(f"Found {len(jobs)} jobs for project {project_id}")
        return jobs


    def cleanup_old_jobs(self, hours: int = 72):
        """
        Delete job records older than specified hours
        
        Args:
            hours: Number of hours after which jobs should be deleted (default: 72)
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        result = self.jobsCollection.delete_many({
            'created_at': {'$lt': cutoff_time}
        })
        deleted_count = result.deleted_count
        print(f"Cleaned up {deleted_count} jobs older than {hours} hours")
        return deleted_count