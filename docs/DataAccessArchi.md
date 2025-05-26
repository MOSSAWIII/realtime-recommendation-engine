# Data Access Architecture

## Database Overview
The Realtime Recommendation Engine is built upon a PostgreSQL database named "recommendation_engine" which serves as the foundation for all persistent data storage. This database contains essential tables:
- "utilisateur" for user profiles
- "evenement" for event details
- "amitie" for direct friendship relationships
- "reseau_amis" for aggregated friend networks
- "participation_evenement" for event participation records

## Database Setup
The database structure was initially created using SQL scripts located in the `sql-tables` directory:
- create-all-tables.sql
- import-users.sql
- import-events.sql
- import-friends.sql
- import-event-attendees.sql

## Data Access Layer

### DataManager Overview
The system employs a centralized approach through the DataManager class, which serves as the single point of access for all PostgreSQL operations, consolidating what was previously handled by multiple repository implementations.

### Connection Management
- Uses HikariCP for efficient connection pooling
- Configuration parameters defined in `application.conf`
- Controls settings like connection URL, credentials, pool size, timeouts

## Core Operations

### User Operations
- getUserProfile
- upsertUser
- getUsersByIds
- deleteUser

### Event Operations
- getEvent
- getAllActiveEvents
- upsertEvent
- deleteEventByCreator

### Participation Management
- addEventParticipation
- getEventParticipations
- getUserParticipations

### User Interaction Processing
- storeUserEvent
- storeProcessedEvent
- getProcessedEventsForUser
- batchStoreProcessedEvents

### Social Graph Operations
- getUserFriends
- upsertUserFriends

### Recommendation Operations
- storeRecommendation
- getRecommendationForUser
- getRecentRecommendationForUser

## Performance Optimizations

### Database Performance Features
- Prepared statement caching
- Memory caching with configurable TTL
- Batch operations for bulk processing
- Automatic resource management
- Health monitoring with database metrics
- Optimized indexes for:
  - Event status
  - Timestamps
  - User IDs
  - Friend relationships

## Transaction Management
All database operations are executed within proper transaction boundaries managed by the DataManagerTransactions class, ensuring data consistency through commit and rollback support.

