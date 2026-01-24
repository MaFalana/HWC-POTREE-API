# Orthophoto Upload - Frontend Implementation Guide

## API Endpoint

```
POST /projects/{project_id}/ortho
```

**Content-Type:** `multipart/form-data`

**Form Field:** `file` (the georeferenced raster file)

**Supported Formats:**
- GeoTIFF (.tif, .tiff)
- JPEG with world file (.jpg, .jpeg + .jgw)
- PNG with world file (.png + .pgw)
- Any format with .wld world file

**Max File Size:** 30GB

---

## Vanilla JavaScript Example

### Basic Upload with Progress

```javascript
async function uploadOrtho(projectId, file) {
  const formData = new FormData();
  formData.append('file', file);
  
  try {
    const response = await fetch(`/projects/${projectId}/ortho`, {
      method: 'POST',
      body: formData
    });
    
    if (!response.ok) {
      throw new Error(`Upload failed: ${response.statusText}`);
    }
    
    const result = await response.json();
    console.log('Upload successful:', result);
    
    // Result contains:
    // {
    //   "message": "Ortho upload accepted for processing",
    //   "job_id": "550e8400-e29b-41d4-a716-446655440000",
    //   "project_id": "PROJ-001",
    //   "status": "pending",
    //   "created_at": "2024-01-15T10:30:00Z"
    // }
    
    return result;
  } catch (error) {
    console.error('Upload error:', error);
    throw error;
  }
}

// Usage
const fileInput = document.getElementById('ortho-file');
fileInput.addEventListener('change', async (e) => {
  const file = e.target.files[0];
  if (file) {
    const result = await uploadOrtho('PROJ-001', file);
    // Poll job status
    pollJobStatus(result.job_id);
  }
});
```

### With Upload Progress Bar

```javascript
function uploadOrthoWithProgress(projectId, file, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    const formData = new FormData();
    formData.append('file', file);
    
    // Track upload progress
    xhr.upload.addEventListener('progress', (e) => {
      if (e.lengthComputable) {
        const percentComplete = (e.loaded / e.total) * 100;
        onProgress(percentComplete);
      }
    });
    
    // Handle completion
    xhr.addEventListener('load', () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(JSON.parse(xhr.responseText));
      } else {
        reject(new Error(`Upload failed: ${xhr.statusText}`));
      }
    });
    
    // Handle errors
    xhr.addEventListener('error', () => {
      reject(new Error('Upload failed'));
    });
    
    xhr.open('POST', `/projects/${projectId}/ortho`);
    xhr.send(formData);
  });
}

// Usage
const progressBar = document.getElementById('progress-bar');
const result = await uploadOrthoWithProgress('PROJ-001', file, (percent) => {
  progressBar.style.width = `${percent}%`;
  progressBar.textContent = `${Math.round(percent)}%`;
});
```

### Poll Job Status

```javascript
async function pollJobStatus(jobId, onUpdate) {
  const poll = async () => {
    try {
      const response = await fetch(`/jobs/${jobId}`);
      const job = await response.json();
      
      onUpdate(job);
      
      // Continue polling if job is not complete
      if (job.status === 'pending' || job.status === 'processing') {
        setTimeout(poll, 2000); // Poll every 2 seconds
      } else if (job.status === 'completed') {
        console.log('Job completed successfully!');
        // Refresh project data to get ortho URLs
        await refreshProject(job.project_id);
      } else if (job.status === 'failed') {
        console.error('Job failed:', job.error_message);
      }
    } catch (error) {
      console.error('Error polling job status:', error);
    }
  };
  
  poll();
}

// Usage
pollJobStatus(result.job_id, (job) => {
  console.log(`Status: ${job.status} - ${job.progress_message}`);
  document.getElementById('status').textContent = job.progress_message;
});
```

---

## React Example

### Upload Component

```jsx
import { useState } from 'react';

function OrthoUpload({ projectId, onUploadComplete }) {
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [jobStatus, setJobStatus] = useState(null);
  const [error, setError] = useState(null);
  
  const handleFileChange = (e) => {
    const selectedFile = e.target.files[0];
    if (selectedFile) {
      // Validate file type
      const validExtensions = ['.tif', '.tiff', '.jpg', '.jpeg', '.png'];
      const fileExt = selectedFile.name.toLowerCase().match(/\.[^.]+$/)?.[0];
      
      if (!validExtensions.includes(fileExt)) {
        setError(`Invalid file type. Supported: ${validExtensions.join(', ')}`);
        return;
      }
      
      // Validate file size (30GB)
      const maxSize = 30 * 1024 * 1024 * 1024;
      if (selectedFile.size > maxSize) {
        setError('File size exceeds 30GB limit');
        return;
      }
      
      setFile(selectedFile);
      setError(null);
    }
  };
  
  const uploadFile = () => {
    if (!file) return;
    
    setUploading(true);
    setError(null);
    
    const xhr = new XMLHttpRequest();
    const formData = new FormData();
    formData.append('file', file);
    
    // Upload progress
    xhr.upload.addEventListener('progress', (e) => {
      if (e.lengthComputable) {
        const percent = (e.loaded / e.total) * 100;
        setUploadProgress(percent);
      }
    });
    
    // Upload complete
    xhr.addEventListener('load', () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        const result = JSON.parse(xhr.responseText);
        setUploading(false);
        setUploadProgress(100);
        
        // Start polling job status
        pollJobStatus(result.job_id);
      } else {
        setError(`Upload failed: ${xhr.statusText}`);
        setUploading(false);
      }
    });
    
    // Upload error
    xhr.addEventListener('error', () => {
      setError('Upload failed');
      setUploading(false);
    });
    
    xhr.open('POST', `/projects/${projectId}/ortho`);
    xhr.send(formData);
  };
  
  const pollJobStatus = async (jobId) => {
    try {
      const response = await fetch(`/jobs/${jobId}`);
      const job = await response.json();
      
      setJobStatus(job);
      
      if (job.status === 'pending' || job.status === 'processing') {
        setTimeout(() => pollJobStatus(jobId), 2000);
      } else if (job.status === 'completed') {
        onUploadComplete?.(job);
      } else if (job.status === 'failed') {
        setError(job.error_message || 'Processing failed');
      }
    } catch (err) {
      console.error('Error polling job:', err);
    }
  };
  
  return (
    <div className="ortho-upload">
      <h3>Upload Orthophoto</h3>
      
      <input
        type="file"
        accept=".tif,.tiff,.jpg,.jpeg,.png"
        onChange={handleFileChange}
        disabled={uploading}
      />
      
      {file && (
        <div className="file-info">
          <p>Selected: {file.name}</p>
          <p>Size: {(file.size / (1024 * 1024)).toFixed(2)} MB</p>
        </div>
      )}
      
      <button
        onClick={uploadFile}
        disabled={!file || uploading}
      >
        {uploading ? 'Uploading...' : 'Upload'}
      </button>
      
      {uploading && (
        <div className="progress-bar">
          <div
            className="progress-fill"
            style={{ width: `${uploadProgress}%` }}
          >
            {Math.round(uploadProgress)}%
          </div>
        </div>
      )}
      
      {jobStatus && (
        <div className="job-status">
          <p>Status: {jobStatus.status}</p>
          <p>{jobStatus.progress_message}</p>
        </div>
      )}
      
      {error && (
        <div className="error">{error}</div>
      )}
    </div>
  );
}

export default OrthoUpload;
```

### Usage in Parent Component

```jsx
function ProjectPage({ projectId }) {
  const [project, setProject] = useState(null);
  
  const refreshProject = async () => {
    const response = await fetch(`/projects/${projectId}`);
    const data = await response.json();
    setProject(data);
  };
  
  const handleUploadComplete = async (job) => {
    console.log('Ortho processing completed!');
    await refreshProject();
  };
  
  return (
    <div>
      <h1>Project: {project?.name}</h1>
      
      <OrthoUpload
        projectId={projectId}
        onUploadComplete={handleUploadComplete}
      />
      
      {project?.ortho && (
        <div className="ortho-preview">
          <h3>Orthophoto</h3>
          <img src={project.ortho.thumbnail} alt="Ortho thumbnail" />
          <a href={project.ortho.url} target="_blank">View Full Size</a>
        </div>
      )}
    </div>
  );
}
```

---

## Vue.js Example

```vue
<template>
  <div class="ortho-upload">
    <h3>Upload Orthophoto</h3>
    
    <input
      type="file"
      accept=".tif,.tiff,.jpg,.jpeg,.png"
      @change="handleFileChange"
      :disabled="uploading"
    />
    
    <div v-if="file" class="file-info">
      <p>Selected: {{ file.name }}</p>
      <p>Size: {{ (file.size / (1024 * 1024)).toFixed(2) }} MB</p>
    </div>
    
    <button @click="uploadFile" :disabled="!file || uploading">
      {{ uploading ? 'Uploading...' : 'Upload' }}
    </button>
    
    <div v-if="uploading" class="progress-bar">
      <div class="progress-fill" :style="{ width: uploadProgress + '%' }">
        {{ Math.round(uploadProgress) }}%
      </div>
    </div>
    
    <div v-if="jobStatus" class="job-status">
      <p>Status: {{ jobStatus.status }}</p>
      <p>{{ jobStatus.progress_message }}</p>
    </div>
    
    <div v-if="error" class="error">{{ error }}</div>
  </div>
</template>

<script>
export default {
  props: {
    projectId: {
      type: String,
      required: true
    }
  },
  data() {
    return {
      file: null,
      uploading: false,
      uploadProgress: 0,
      jobStatus: null,
      error: null
    };
  },
  methods: {
    handleFileChange(e) {
      const selectedFile = e.target.files[0];
      if (selectedFile) {
        const validExtensions = ['.tif', '.tiff', '.jpg', '.jpeg', '.png'];
        const fileExt = selectedFile.name.toLowerCase().match(/\.[^.]+$/)?.[0];
        
        if (!validExtensions.includes(fileExt)) {
          this.error = `Invalid file type. Supported: ${validExtensions.join(', ')}`;
          return;
        }
        
        const maxSize = 30 * 1024 * 1024 * 1024;
        if (selectedFile.size > maxSize) {
          this.error = 'File size exceeds 30GB limit';
          return;
        }
        
        this.file = selectedFile;
        this.error = null;
      }
    },
    
    uploadFile() {
      if (!this.file) return;
      
      this.uploading = true;
      this.error = null;
      
      const xhr = new XMLHttpRequest();
      const formData = new FormData();
      formData.append('file', this.file);
      
      xhr.upload.addEventListener('progress', (e) => {
        if (e.lengthComputable) {
          this.uploadProgress = (e.loaded / e.total) * 100;
        }
      });
      
      xhr.addEventListener('load', () => {
        if (xhr.status >= 200 && xhr.status < 300) {
          const result = JSON.parse(xhr.responseText);
          this.uploading = false;
          this.uploadProgress = 100;
          this.pollJobStatus(result.job_id);
        } else {
          this.error = `Upload failed: ${xhr.statusText}`;
          this.uploading = false;
        }
      });
      
      xhr.addEventListener('error', () => {
        this.error = 'Upload failed';
        this.uploading = false;
      });
      
      xhr.open('POST', `/projects/${this.projectId}/ortho`);
      xhr.send(formData);
    },
    
    async pollJobStatus(jobId) {
      try {
        const response = await fetch(`/jobs/${jobId}`);
        const job = await response.json();
        
        this.jobStatus = job;
        
        if (job.status === 'pending' || job.status === 'processing') {
          setTimeout(() => this.pollJobStatus(jobId), 2000);
        } else if (job.status === 'completed') {
          this.$emit('upload-complete', job);
        } else if (job.status === 'failed') {
          this.error = job.error_message || 'Processing failed';
        }
      } catch (err) {
        console.error('Error polling job:', err);
      }
    }
  }
};
</script>
```

---

## Complete Workflow

1. **Upload File** → `POST /projects/{project_id}/ortho`
   - Returns `job_id`

2. **Poll Job Status** → `GET /jobs/{job_id}`
   - Check every 2-5 seconds
   - Status: `pending` → `processing` → `completed`

3. **Get Updated Project** → `GET /projects/{project_id}`
   - Project now has `ortho` object with `url`, `thumbnail`, and `bounds`

4. **Display on Map** → Use Leaflet `imageOverlay`
   ```javascript
   L.imageOverlay(project.ortho.url, project.ortho.bounds).addTo(map);
   ```

---

## Error Handling

### Common Errors

**400 Bad Request**
- Invalid file type
- Missing file

**404 Not Found**
- Project doesn't exist

**413 Payload Too Large**
- File exceeds 30GB

**500 Internal Server Error**
- Server error during upload

### Example Error Handler

```javascript
async function uploadOrthoWithErrorHandling(projectId, file) {
  try {
    const formData = new FormData();
    formData.append('file', file);
    
    const response = await fetch(`/projects/${projectId}/ortho`, {
      method: 'POST',
      body: formData
    });
    
    if (!response.ok) {
      const error = await response.json();
      
      switch (response.status) {
        case 400:
          throw new Error(`Invalid file: ${error.detail}`);
        case 404:
          throw new Error('Project not found');
        case 413:
          throw new Error('File too large (max 30GB)');
        default:
          throw new Error(`Upload failed: ${error.detail}`);
      }
    }
    
    return await response.json();
  } catch (error) {
    console.error('Upload error:', error);
    throw error;
  }
}
```

---

## Tips

1. **Validate file type client-side** before uploading
2. **Show upload progress** for large files
3. **Poll job status** every 2-5 seconds (not too frequently)
4. **Handle job failures** gracefully with error messages
5. **Refresh project data** after job completes to get ortho URLs
6. **Cache ortho URLs** to avoid repeated API calls
7. **Consider drag-and-drop** for better UX
