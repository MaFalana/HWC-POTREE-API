# Design Document

## Overview

This design document outlines the technical approach for implementing job cancellation, pagination, sorting, search/filtering, and statistics capabilities in the HWC Potree API. The enhancements will be implemented in a backward-compatible manner, ensuring existing API clients continue to function without modification while providing new capabilities for improved user experience and performance.

The design follows RESTful principles and leverages MongoDB's aggregation framework for efficient queries. All enhancements are designed to work seamlessly with the existing FastAPI backend and Azure Cosmos DB for MongoDB.

## Architecture

### System Components

```
┌─────────────────┐
│  Astro Frontend │
└────────┬────────┘
         │ HTTP/REST
         ▼
┌───────────────────────────┐
│         │
│  ┌─────────────────────────┐  │
│  │  Routes Layer                    │  │
│  │  - /projects/ (enhanced)         │  │
│  │  - /jobs/{id}/cancel (new)       │  │
│  │  - /stats (

│            │
│  ┌─────  │
│  │  Business Logic Layer            │  │
│  │  - QueryBuilder                │
│  │  - CancellationHandler           │  │
│  │  - StatisticsAggregator          │  │
│  └──────────────┬───────────────────┘  │
  │
│  ┌──────────────▼──

│  │  │
│  │  - Index management              │  │
│  └──────────────┬───────────────────┘  │
└──────┘
                  │
         ┌────────┐
    ▼
┌─┐
│   MongoDB    │   │ Azure Blob   │
│  (Cosmos DB) │   │   Stoe    │
└──────────────┘   └────────
         ▲
         │
┌────────┴────────┐
  │
│  (Background)   │

│    cancellation │

└─────────────────┘
```

### Data Flow

**1. Job Cancellati
```
User → Frontend → POST /jobs/{id}/cancel → API vate
                       DB
nfirmation
                       
Worker → Polls for jobs → Checks cancelledtep
                    
                        → If not: c
```


```
User → Frontend t=0
                → API builds Mters
                → Applies sorting
imit)
                → Counts total
                → Returns pagi
```

**3. Statistics Flow:**

User → Frontend → GET /stats
                → API runipeline
                →
                → Returns aggregated statistics
```

## Components and Interaces

### 1. Enhanced Project Routes

The GET /projects/ endpoint will

**Query Parameter**
ax 100)
- offset: Number of projeault 0)
- sort_by: Field to sort by (created_atent)
- sort_order: Sort order t desc)
- search: Search i
nt name
- tags: Comma-separated tags (OR logic)

**Response Structure includes pagination meta

### 2. Job Cancellation s

New POST endpoint at /jobs/{jon flag.

Returns success response with job details or conflict erro

outes

s.

###ger

New metds added:
- get_projects_paginated(): sorting
- cancel_job(): Sets cancellation flag and updates job status
- is_job_cancelled(): Lightweight checus
- getines

Enhanced

###

Work

If cancellation detected,up:
- Deletes local temporary file
- Deletes Azure job file
- Deletes partial Potree output fils
-led

s.

## Data Mls

###el

Added cancelled boolean field (default false) to t

Se.

### Project l

g needs.

#ing

- 400 
ound
- 409 Conflict: Cannot cancel completed/failed job
- 500 Internal Server Error: d errors

All erro.

## Testing Strategy

### Unit Tests

- Cancellation state transitions
ons
- Pagination lic

### Integration Tests
w
- Search and filter combinations
- Cn
- Scy

### Performance Tests
- Pagination with 10,000+ projects (target <200ms)
- Search perfo
- Stat00ms)

## Deployment Considerations

#s

Add cancelled fieldse).

Create new indexes on projects collection for name, client, tags, and text search.

### Backward Com

All changes are additive - existing clients continue to work without modification.

New query parts.

Response structure maintains Projects array in same location with added pagination metadata.

### Monitoring

Track metrics for cancellation rate, query peage.

Set up al

### Rollback Plan

All changes can

Remove se.

Drop new indexes if causing performance problems.
