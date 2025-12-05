# Ortho Upload Feature - Rollback Plan

This document outlines the rollback procedures for the orthophoto upload and COG conversion feature.

## Overview

The ortho upload feature is designed to be backward compatible. Rollback is straightforward because:

- **No database migration required** - The `ortho` field is optional in the Project model
- **No breaking API changes** - All existing endpoints continue to work
- **Isolated feature** - Ortho processing doesn't affect point cloud processing
- **Graceful degradation** - Projects without ortho data work normally

## Rollback Scenarios

### Scenario 1: Critical Bug in Production

**When to use:** A critical bug is discovered that affects system stability or data integrity.

**Impact:** High - Immediate rollback required

**Steps:**

1. **Revert to previous version**

   ```bash
   # Docker deployment
   docker pull hwc-potree-api:previous-version
   docker stop hwc-potree-api
   docker run -d --name hwc-potree-api hwc-potree-api:previous-version

   # Or use your deployment tool
   kubectl rollout undo deployment/hwc-potree-api
   ```

2. **Verify rollback**

   ```bash
   # Check API is responding
   curl http://your-api-url/health

   # Verify ortho endpoint is gone
   curl http://your-api-url/ | grep ortho
   ```

3. **Handle in-progress jobs**

   - In-progress ortho jobs will fail with "Job type not supported"
   - These jobs can be manually marked as failed or left to timeout
   - No data corruption will occur

4. **Communicate with users**
   - Notify users that ortho upload is temporarily unavailable
   - Provide timeline for fix and redeployment

**Rollback time:** 5-10 minutes

**Data impact:** None - existing ortho data remains in Azure and database

### Scenario 2: Performance Issues

**When to use:** Ortho processing is causing performance degradation for point cloud processing.

**Impact:** Medium - Can be addressed with configuration changes first

**Steps:**

1. **Try configuration fixes first**

   - Reduce concurrent ortho job processing
   - Increase worker memory allocation
   - Adjust GDAL compression settings

2. **If configuration doesn't help, rollback**
   - Follow steps from Scenario 1
   - Investigate performance issues offline
   - Optimize and redeploy

**Rollback time:** 5-10 minutes (if needed)

**Data impact:** None

### Scenario 3: GDAL Dependency Issues

**When to use:** GDAL installation or compatibility issues in production.

**Impact:** Medium - Affects only ortho uploads, not existing functionality

**Steps:**

1. **Attempt to fix GDAL installation**

   ```bash
   # SSH into server/container
   apt-get update
   apt-get install --reinstall gdal-bin libgdal-dev

   # Verify
   gdalinfo --version
   python -c "from osgeo import gdal; print(gdal.__version__)"
   ```

2. **If fix doesn't work, rollback**
   - Follow steps from Scenario 1
   - Fix GDAL issues in staging environment
   - Redeploy when fixed

**Rollback time:** 5-10 minutes

**Data impact:** None

### Scenario 4: Partial Rollback (Disable Feature Only)

**When to use:** Want to keep new code but disable ortho upload temporarily.

**Impact:** Low - Feature flag approach

**Steps:**

1. **Add feature flag to environment**

   ```bash
   # Add to environment variables
   ORTHO_UPLOAD_ENABLED=false
   ```

2. **Update endpoint to check flag**

   ```python
   # In routes/projects.py
   if not os.getenv('ORTHO_UPLOAD_ENABLED', 'true').lower() == 'true':
       raise HTTPException(503, "Ortho upload temporarily disabled")
   ```

3. **Restart service**
   ```bash
   # Restart to pick up new environment variable
   docker restart hwc-potree-api
   ```

**Rollback time:** 2-3 minutes

**Data impact:** None

## Backward Compatibility Testing

Before deploying, verify backward compatibility:

### Test 1: Existing Projects Work

```bash
# Get a project created before ortho feature
curl http://your-api-url/projects/OLD-PROJECT-ID

# Should return project without ortho field or with ortho: null
# Should NOT error
```

### Test 2: Point Cloud Upload Still Works

```bash
# Upload a point cloud to existing project
curl -X POST "http://your-api-url/projects/OLD-PROJECT-ID/upload" \
  -F "file=@test.las"

# Should work normally
```

### Test 3: Project Listing Works

```bash
# List all projects
curl http://your-api-url/projects/

# Should include both old projects (no ortho) and new projects (with ortho)
# Should NOT error on projects without ortho field
```

### Test 4: Worker Processes Point Cloud Jobs

```bash
# Create point cloud job
# Verify worker processes it normally
# Ortho feature should not interfere
```

## Data Preservation

### What Happens to Ortho Data After Rollback?

**In Azure Storage:**

- Ortho files remain in Azure at `{project_id}/ortho/`
- Files are not deleted during rollback
- Files can be accessed directly via Azure portal if needed
- Files will be available when feature is redeployed

**In MongoDB:**

- Project documents retain `ortho` field with URLs
- Old API version ignores unknown fields (MongoDB flexibility)
- Data is preserved for when feature is redeployed

**In Job Queue:**

- In-progress ortho jobs will fail
- Completed ortho jobs remain in history
- Failed jobs can be retried after redeployment

### Manual Data Cleanup (If Needed)

If you need to remove ortho data after rollback:

```python
# Remove ortho field from all projects
from pymongo import MongoClient

client = MongoClient(MONGODB_URI)
db = client.hwc_potree
db.projects.update_many(
    {},
    {'$unset': {'ortho': ''}}
)

# Delete ortho files from Azure (optional)
# Use Azure portal or Azure CLI to delete {project_id}/ortho/ folders
```

## Rollback Verification Checklist

After rollback, verify:

- [ ] API health endpoint responds
- [ ] Root endpoint lists available endpoints (ortho should be gone)
- [ ] Existing projects can be retrieved
- [ ] Point cloud upload still works
- [ ] Worker processes point cloud jobs
- [ ] No errors in application logs
- [ ] Database queries work normally
- [ ] Azure storage is accessible

## Communication Plan

### Internal Team

1. Notify DevOps team of rollback
2. Update incident tracking system
3. Schedule post-mortem if needed
4. Plan fix and redeployment timeline

### External Users

1. Update status page if available
2. Send email notification if ortho feature was announced
3. Provide timeline for fix
4. Offer alternative workflows if available

## Prevention Measures

To minimize need for rollback:

1. **Thorough staging testing**

   - Test with various file sizes
   - Test with different GeoTIFF formats
   - Load test with concurrent uploads
   - Test cancellation scenarios

2. **Gradual rollout**

   - Deploy to staging first
   - Deploy to production with monitoring
   - Consider canary deployment
   - Monitor metrics closely for first 24 hours

3. **Feature flags**

   - Use environment variable to enable/disable
   - Allows quick disable without full rollback

4. **Monitoring and alerts**
   - Set up alerts for job failures
   - Monitor processing times
   - Track memory and disk usage
   - Alert on GDAL errors

## Recovery After Rollback

Once issues are fixed:

1. **Fix issues in development**

   - Reproduce issue locally
   - Implement fix
   - Add tests to prevent regression

2. **Test thoroughly in staging**

   - Run full test suite
   - Run deployment verification
   - Test specific issue that caused rollback

3. **Redeploy with caution**

   - Deploy during low-traffic period
   - Monitor closely
   - Have rollback plan ready
   - Communicate with team

4. **Verify in production**
   - Run smoke tests
   - Check metrics
   - Verify ortho uploads work
   - Monitor for 24 hours

## Rollback Decision Matrix

| Issue Type              | Severity | Action                               | Rollback Time |
| ----------------------- | -------- | ------------------------------------ | ------------- |
| API crashes             | Critical | Immediate rollback                   | 5 min         |
| Data corruption         | Critical | Immediate rollback                   | 5 min         |
| Security vulnerability  | Critical | Immediate rollback                   | 5 min         |
| Ortho jobs failing      | High     | Investigate, then rollback if needed | 15-30 min     |
| Performance degradation | Medium   | Try config fixes, then rollback      | 30-60 min     |
| GDAL errors             | Medium   | Try fixes, then rollback             | 30-60 min     |
| Minor UI issues         | Low      | Fix forward, no rollback             | N/A           |
| Slow processing         | Low      | Optimize, no rollback                | N/A           |

## Contact Information

**On-Call Engineer:** [Your on-call rotation]

**DevOps Team:** [Your DevOps contact]

**Database Admin:** [Your DBA contact]

**Escalation Path:**

1. On-call engineer
2. Team lead
3. Engineering manager

## Appendix: Version History

| Version | Date     | Changes                      | Rollback From |
| ------- | -------- | ---------------------------- | ------------- |
| 1.0.0   | TBD      | Initial ortho upload feature | Any version   |
| 0.9.x   | Previous | Pre-ortho versions           | N/A           |

## Testing This Rollback Plan

Periodically test the rollback procedure:

1. Deploy ortho feature to staging
2. Create test ortho uploads
3. Perform rollback
4. Verify all checklist items
5. Document any issues or improvements needed
6. Update this plan based on learnings

**Last tested:** [Date]

**Test result:** [Pass/Fail]

**Notes:** [Any observations]
