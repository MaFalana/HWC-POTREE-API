# User Guide - Orthophoto Upload and COG Conversion

## Table of Contents

- [Introduction](#introduction)
- [What is an Orthophoto?](#what-is-an-orthophoto)
- [What is Cloud Optimized GeoTIFF (COG)?](#what-is-cloud-optimized-geotiff-cog)
- [Getting Started](#getting-started)
- [Uploading an Orthophoto](#uploading-an-orthophoto)
- [Monitoring Processing Status](#monitoring-processing-status)
- [Viewing Your Orthophoto](#viewing-your-orthophoto)
- [Managing Orthophotos](#managing-orthophotos)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)

---

## Introduction

The HWC Potree API now supports orthophoto upload and automatic conversion to Cloud Optimized GeoTIFF (COG) format. This feature allows you to:

- Upload GeoTIFF orthophotos for your projects
- Automatically convert them to web-optimized COG format
- Generate thumbnail previews for quick identification
- View orthophotos alongside point cloud data
- Stream large orthophotos efficiently in web viewers

This guide will walk you through the entire workflow from uploading your first orthophoto to viewing it in your application.

---

## What is an Orthophoto?

An **orthophoto** (or orthophotograph) is an aerial photograph that has been geometrically corrected so that the scale is uniform throughout the image. This means:

- **No distortion** - Buildings and terrain appear as they would on a map
- **Accurate measurements** - You can measure distances and areas directly
- **Georeferenced** - The image is tied to real-world coordinates
- **Combines with other data** - Can be overlaid with point clouds, vectors, etc.

**Common Uses:**

- Base maps for GIS applications
- Visual context for LiDAR point clouds
- Change detection and monitoring
- Planning and design visualization
- Asset management and inspection

---

## What is Cloud Optimized GeoTIFF (COG)?

A **Cloud Optimized GeoTIFF (COG)** is a regular GeoTIFF file with an internal organization that enables efficient workflows in cloud environments.

### Benefits of COG

**1. Efficient Streaming**

- Only download the parts of the image you need
- No need to download the entire file to view a small area
- Faster loading in web viewers

**2. Tiled Structure**

- Image is divided into small tiles (512x512 pixels)
- Tiles are loaded on-demand as you pan and zoom
- Smooth viewing experience even for large files

**3. Multiple Resolutions**

- Contains built-in overviews (pyramids)
- Quick display at low zoom levels
- Progressive loading from low to high resolution

**4. Web-Optimized**

- Works with standard HTTP range requests
- Compatible with modern web mapping libraries
- No special server software required

**5. Compression**

- JPEG compression reduces file size
- Quality 85 provides good balance of size and quality
- Faster uploads and downloads

### Why We Convert to COG

When you upload a GeoTIFF, we automatically convert it to COG format because:

- **Better Performance** - Your orthophotos load faster in web viewers
- **Lower Bandwidth** - Only download what you need to see
- **Scalability** - Handle large files (10GB+) without issues
- **Standard Format** - Works with all modern GIS tools
- **Future-Proof** - Industry standard for cloud-based geospatial data

---

## Getting Started

### Prerequisites

Before uploading an orthophoto, you need:

1. **A Project** - Create a project first using the API or web interface
2. **A GeoTIFF File** - Your orthophoto in .tif or .tiff format
3. **Valid Georeferencing** - The file must have coordinate system information

### File Requirements

**Format:**

- File extension: `.tif` or `.tiff`
- Must be a valid GeoTIFF (readable by GDAL)
- Must contain georeferencing information

**Size:**

- Maximum: 30GB
- Recommended: < 10GB for faster processing
- Minimum: No minimum, but very small files may not benefit from COG

**Coordinate System:**

- Any projected or geographic coordinate system
- Must be defined in the GeoTIFF metadata
- Common systems: UTM, State Plane, Web Mercator

**Bands:**

- Single-band (grayscale)
- Multi-band (RGB, RGBA)
- Any bit depth (8-bit, 16-bit, 32-bit)

---

## Uploading an Orthophoto

### Step 1: Prepare Your File

Before uploading, verify your file is valid:

**Using QGIS:**

1. Open QGIS
2. Drag and drop your GeoTIFF file
3. Verify it displays correctly
4. Check the coordinate system in the layer properties

**Using GDAL (Command Line):**

```bash
gdalinfo your_orthophoto.tif
```

Look for:

- "Driver: GTiff/GeoTIFF" - Confirms it's a GeoTIFF
- "Coordinate System" - Shows the projection
- "Size is X, Y" - Shows dimensions
- No error messages

### Step 2: Upload via API

**Using cURL:**

```bash
curl -X POST "https://your-api-url.com/projects/PROJ-001/ortho" \
  -F "file=@your_orthophoto.tif"
```

**Using Python:**

```python
import requests

url = "https://your-api-url.com/projects/PROJ-001/ortho"
files = {'file': open('your_orthophoto.tif', 'rb')}
response = requests.post(url, files=files)

if response.status_code == 202:
    job_id = response.json()['job_id']
    print(f"Upload successful! Job ID: {job_id}")
else:
    print(f"Upload failed: {response.text}")
```

**Using JavaScript:**

```javascript
const formData = new FormData();
formData.append("file", fileInput.files[0]);

const response = await fetch(
  "https://your-api-url.com/projects/PROJ-001/ortho",
  {
    method: "POST",
    body: formData,
  }
);

if (response.ok) {
  const data = await response.json();
  console.log("Upload successful! Job ID:", data.job_id);
} else {
  console.error("Upload failed:", await response.text());
}
```

### Step 3: Receive Job ID

After successful upload, you'll receive a response like:

```json
{
  "message": "Ortho upload accepted for processing",
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "project_id": "PROJ-001",
  "status": "pending",
  "created_at": "2024-01-15T10:30:00Z"
}
```

**Save the `job_id`** - You'll need it to check processing status.

---

## Monitoring Processing Status

### Understanding Processing Steps

Your orthophoto goes through several processing steps:

1. **Pending** (0-30 seconds)

   - Job is queued and waiting for worker
   - No action required

2. **Downloading file** (30 seconds - 2 minutes)

   - Worker downloads file from temporary storage
   - Duration depends on file size

3. **Validating file** (10-30 seconds)

   - GDAL verifies the file is a valid GeoTIFF
   - Checks for georeferencing information

4. **Converting to COG** (2-30 minutes)

   - Main processing step
   - Converts to Cloud Optimized GeoTIFF
   - Applies JPEG compression
   - Creates tiled structure
   - Duration depends on file size and complexity

5. **Generating thumbnail** (10-30 seconds)

   - Creates 512px wide preview image
   - Optional - won't fail job if it fails

6. **Uploading to Azure** (1-5 minutes)

   - Uploads COG and thumbnail to cloud storage
   - Generates secure access URLs

7. **Completed**
   - Project is updated with ortho URLs
   - Ready to view!

### Checking Status

**Poll the job status endpoint:**

```bash
curl "https://your-api-url.com/jobs/550e8400-e29b-41d4-a716-446655440000"
```

**Response:**

```json
{
  "_id": "550e8400-e29b-41d4-a716-446655440000",
  "project_id": "PROJ-001",
  "type": "ortho_conversion",
  "status": "processing",
  "progress_message": "Converting to COG",
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-01-15T10:35:00Z"
}
```

**Status Values:**

- `pending` - Waiting to start
- `processing` - Currently processing (check `progress_message` for details)
- `completed` - Successfully finished
- `failed` - Processing failed (check `error_message`)
- `cancelled` - Cancelled by user

### Automated Polling

**JavaScript Example:**

```javascript
async function pollJobStatus(jobId) {
  while (true) {
    const response = await fetch(`https://your-api-url.com/jobs/${jobId}`);
    const job = await response.json();

    console.log(`Status: ${job.status} - ${job.progress_message || ""}`);

    if (job.status === "completed") {
      console.log("Processing complete!");
      return job;
    } else if (job.status === "failed") {
      throw new Error(`Processing failed: ${job.error_message}`);
    }

    // Wait 2 seconds before checking again
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }
}

// Usage
const job = await pollJobStatus("550e8400-e29b-41d4-a716-446655440000");
```

### Expected Processing Times

| File Size | Typical Processing Time |
| --------- | ----------------------- |
| < 1GB     | 2-5 minutes             |
| 1-5GB     | 5-15 minutes            |
| 5-10GB    | 15-25 minutes           |
| 10-30GB   | 25-45 minutes           |

**Note:** Times vary based on:

- File complexity and compression
- Server load
- Network speed
- Number of concurrent jobs

---

## Viewing Your Orthophoto

### Accessing Ortho URLs

Once processing is complete, get the updated project:

```bash
curl "https://your-api-url.com/projects/PROJ-001"
```

**Response includes ortho URLs:**

```json
{
  "_id": "PROJ-001",
  "name": "Highway Survey",
  "client": "DOT",
  "ortho": {
    "url": "https://storage.blob.core.windows.net/container/project-id/ortho/ortho.png",
    "thumbnail": "https://storage.blob.core.windows.net/container/project-id/ortho/ortho_thumbnail.png",
    "bounds": [[south, west], [north, east]]
  }
}
```

### Using the COG File

**In Web Mapping Libraries:**

The COG file can be used with modern web mapping libraries:

**Leaflet with georaster-layer-for-leaflet:**

```javascript
import GeoRasterLayer from "georaster-layer-for-leaflet";
import parseGeoraster from "georaster";

// Fetch and parse the COG
const response = await fetch(project.ortho.file);
const arrayBuffer = await response.arrayBuffer();
const georaster = await parseGeoraster(arrayBuffer);

// Add to map
const layer = new GeoRasterLayer({
  georaster: georaster,
  opacity: 0.7,
});
layer.addTo(map);
```

**OpenLayers:**

```javascript
import GeoTIFF from "ol/source/GeoTIFF";
import TileLayer from "ol/layer/WebGLTile";

const source = new GeoTIFF({
  sources: [
    {
      url: project.ortho.file,
    },
  ],
});

const layer = new TileLayer({
  source: source,
});

map.addLayer(layer);
```

### Using the Thumbnail

The thumbnail is perfect for:

- Project list previews
- Quick identification
- Gallery views
- Mobile applications

**Display in HTML:**

```html
<img src="${project.ortho.thumbnail}" alt="Orthophoto preview" />
```

### Public URLs

**Important:** All URLs are now public and permanent - they never expire!

URLs are direct links to Azure Blob Storage:

```javascript
// Example URL format
const url = "https://storage.blob.core.windows.net/container/project-id/ortho.png";

// No expiration checking needed - URLs are permanent
const response = await fetch(
    `https://your-api-url.com/projects/${projectId}`
  );
  project = await response.json();
}
```

---

## Managing Orthophotos

### Replacing an Orthophoto

To replace an existing orthophoto, simply upload a new one:

```bash
curl -X POST "https://your-api-url.com/projects/PROJ-001/ortho" \
  -F "file=@new_orthophoto.tif"
```

**What happens:**

- Old COG file is overwritten
- Old thumbnail is overwritten
- Project is updated with new URLs
- Old files are automatically cleaned up

### Cancelling Processing

If you uploaded the wrong file, cancel the job:

```bash
curl -X POST "https://your-api-url.com/jobs/{job_id}/cancel"
```

**What happens:**

- Processing stops immediately
- Temporary files are cleaned up
- Job status changes to "cancelled"
- You can upload a new file

**Note:** You can only cancel jobs that are "pending" or "processing". Completed jobs cannot be cancelled.

### Deleting an Orthophoto

To delete an orthophoto, delete the entire project:

```bash
curl -X DELETE "https://your-api-url.com/projects/PROJ-001/delete"
```

**What happens:**

- Project is deleted from database
- All files are deleted from Azure (point cloud, ortho, thumbnails)
- Cannot be undone

**Note:** There's currently no way to delete just the orthophoto while keeping the project and point cloud.

---

## Best Practices

### File Preparation

**1. Optimize Before Upload**

If your file is very large, consider:

- Compressing with GDAL before upload
- Reducing resolution if appropriate
- Removing unnecessary bands

```bash
# Compress GeoTIFF before upload
gdal_translate -co COMPRESS=JPEG -co QUALITY=85 input.tif output.tif
```

**2. Verify Georeferencing**

Always verify your file has valid georeferencing:

```bash
gdalinfo your_file.tif | grep "Coordinate System"
```

If missing, add it using GDAL or QGIS before uploading.

**3. Use Appropriate Coordinate Systems**

- Use projected coordinate systems (UTM, State Plane) for local projects
- Use geographic coordinate systems (WGS84) for global projects
- Match the coordinate system of your point cloud data

### Upload Strategy

**1. Test with Small Files First**

Before uploading large files:

- Test with a small sample (< 1GB)
- Verify the workflow works
- Check the output quality

**2. Upload During Off-Peak Hours**

For large files:

- Upload during nights or weekends
- Reduces competition for resources
- Faster processing times

**3. Monitor Progress**

- Don't close your browser/application immediately
- Poll job status regularly
- Be prepared to wait for large files

### Quality Considerations

**1. Resolution vs. File Size**

Balance resolution with file size:

- Higher resolution = larger files = longer processing
- Consider your viewing needs
- 0.5m resolution is often sufficient for web viewing

**2. Compression Quality**

The system uses JPEG compression at quality 85:

- Good balance of size and quality
- Suitable for most applications
- Minimal visible artifacts

**3. Color Accuracy**

If color accuracy is critical:

- Use high-quality source imagery
- Avoid excessive compression before upload
- Test with sample areas first

---

## Troubleshooting

### Upload Issues

#### Problem: "Invalid file type" error

**Cause:** File extension is not .tif or .tiff

**Solution:**

- Rename file to have .tif extension
- Verify file is actually a GeoTIFF
- Don't just rename a JPEG to .tif - it must be a real GeoTIFF

---

#### Problem: "File too large" error

**Cause:** File exceeds 30GB limit

**Solution:**

- Compress the file before upload
- Split into multiple tiles
- Reduce resolution if appropriate

```bash
# Compress large file
gdal_translate -co COMPRESS=JPEG -co QUALITY=75 large.tif compressed.tif
```

---

#### Problem: "Project not found" error

**Cause:** Project ID doesn't exist

**Solution:**

- Verify project ID is correct
- Check for typos
- Create project first if it doesn't exist

---

### Processing Issues

#### Problem: Job fails with "Invalid GeoTIFF file"

**Cause:** File is not a valid GeoTIFF or is corrupted

**Solution:**

1. Test file with GDAL:
   ```bash
   gdalinfo your_file.tif
   ```
2. Try opening in QGIS
3. If corrupted, re-export from source
4. Ensure file has georeferencing

---

#### Problem: Job stuck in "processing" for hours

**Cause:** Very large file or server issues

**Solution:**

1. Wait longer (30GB files can take 45+ minutes)
2. Check server status
3. If stuck for > 1 hour, cancel and retry
4. Contact support if problem persists

---

#### Problem: Thumbnail not generated

**Cause:** Thumbnail generation failed (optional step)

**Solution:**

- This won't fail the job
- COG file is still created successfully
- Thumbnail URL will be null
- You can still use the COG file

---

#### Problem: Job fails with GDAL error

**Cause:** GDAL couldn't process the file

**Solution:**

1. Check error message for details
2. Verify file format is supported
3. Try converting file format:
   ```bash
   gdal_translate -of GTiff input.tif output.tif
   ```
4. Remove any unusual features (masks, overviews)

---

### Viewing Issues

#### Problem: COG file won't load in web viewer

**Cause:** Various possible causes

**Solution:**

1. Verify URL is accessible (try in browser)
2. Check CORS settings
3. Ensure Azure container is public
4. Verify web viewer supports COG format
5. Try with a different viewer

---

#### Problem: Image appears distorted or wrong location

**Cause:** Coordinate system mismatch

**Solution:**

1. Verify source file coordinate system
2. Check web viewer projection settings
3. Ensure coordinate system is properly defined
4. May need to reproject source file

---

#### Problem: URL not accessible

**Cause:** Network issue or incorrect URL

**Solution:**

```javascript
// Verify URL is correct and accessible
const response = await fetch(`https://your-api-url.com/projects/${projectId}`);
const project = await response.json();
// URLs are public and permanent - no expiration
```

---

## FAQ

### General Questions

**Q: Can I upload multiple orthophotos to one project?**

A: No, currently each project can have only one orthophoto. Uploading a new one will replace the existing one.

---

**Q: What happens to my original file?**

A: The original file is deleted after successful COG conversion. Only the COG and thumbnail are kept.

---

**Q: Can I download the COG file?**

A: Yes, the COG file URL is a direct download link. You can download it with any HTTP client.

---

**Q: How long are the URLs valid?**

A: URLs are public and permanent - they never expire!

---

**Q: Can I use the COG in desktop GIS software?**

A: Yes! COG files work in QGIS, ArcGIS, and other GIS software that supports GeoTIFF.

---

### Technical Questions

**Q: What compression is used?**

A: JPEG compression at quality 85. This provides a good balance of file size and image quality.

---

**Q: What tile size is used?**

A: 512x512 pixels. This is optimal for web streaming.

---

**Q: Are overviews (pyramids) created?**

A: Yes, COG format includes built-in overviews for efficient multi-scale viewing.

---

**Q: Can I specify custom COG options?**

A: No, the system uses optimized default settings. For custom options, convert locally and upload the COG.

---

**Q: What coordinate systems are supported?**

A: Any coordinate system supported by GDAL. The system preserves the original coordinate system.

---

**Q: Can I upload already-optimized COG files?**

A: Yes, but they'll still be processed. The system will re-optimize them with standard settings.

---

### Workflow Questions

**Q: Should I upload the orthophoto before or after the point cloud?**

A: Either order works. They're processed independently.

---

**Q: Can I cancel a job after it starts?**

A: Yes, use the cancel endpoint. Processing will stop and temporary files will be cleaned up.

---

**Q: What if I upload the wrong file?**

A: Cancel the job and upload the correct file. Or wait for it to complete and upload a replacement.

---

**Q: Can I process multiple orthophotos simultaneously?**

A: Yes, the system can process multiple jobs concurrently (for different projects).

---

**Q: How do I know when processing is complete?**

A: Poll the job status endpoint. When status is "completed", it's ready.

---

### Performance Questions

**Q: How long does processing take?**

A: Depends on file size:

- < 1GB: 2-5 minutes
- 1-5GB: 5-15 minutes
- 5-10GB: 15-25 minutes
- 10-30GB: 25-45 minutes

---

**Q: Why is my large file taking so long?**

A: COG conversion is CPU-intensive. Large files require more processing time. This is normal.

---

**Q: Can I speed up processing?**

A: Not directly, but you can:

- Compress files before upload
- Reduce resolution if appropriate
- Upload during off-peak hours

---

**Q: Does file format affect processing time?**

A: Yes. Uncompressed files take longer. Pre-compressed files may process faster.

---

### Troubleshooting Questions

**Q: My job failed. What should I do?**

A:

1. Check the error message in the job status
2. Verify your file with `gdalinfo`
3. Try with a smaller test file
4. Contact support if problem persists

---

**Q: The thumbnail looks wrong. Is the COG file also wrong?**

A: No. Thumbnail generation is separate. If thumbnail fails, the COG is still valid.

---

**Q: Can I retry a failed job?**

A: No, you need to upload the file again. Fix any issues first.

---

**Q: Who do I contact for help?**

A: Check the GitHub repository issues or contact your system administrator.

---

## Additional Resources

### Documentation

- [API Documentation](./API_DOCUMENTATION.md) - Complete API reference
- [Deployment Guide](./DEPLOYMENT_GUIDE.md) - For administrators
- [README](../README.md) - Project overview

### External Resources

- [GDAL Documentation](https://gdal.org/) - GDAL library documentation
- [COG Specification](https://www.cogeo.org/) - Cloud Optimized GeoTIFF specification
- [GeoTIFF Format](https://www.ogc.org/standards/geotiff) - GeoTIFF standard

### Tools

- [QGIS](https://qgis.org/) - Free GIS software for viewing and editing GeoTIFFs
- [GDAL](https://gdal.org/download.html) - Command-line tools for geospatial data
- [rio-cogeo](https://github.com/cogeotiff/rio-cogeo) - Python tool for creating COGs

---

## Support

If you encounter issues not covered in this guide:

1. Check the [Troubleshooting](#troubleshooting) section
2. Review the [FAQ](#faq)
3. Check server logs for detailed error messages
4. Test with the `/health` endpoint
5. Contact your system administrator
6. Open an issue on GitHub: https://github.com/MaFalana/HWC-POTREE-API/issues

---

## Feedback

We're constantly improving this feature. If you have suggestions or feedback:

- Open a GitHub issue
- Contact the development team
- Submit a pull request

Thank you for using the HWC Potree API!
