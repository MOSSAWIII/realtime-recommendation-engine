# To-Do List: Refactoring Database Access Through DataManager Only

## 1. Architecture Changes
- [ ] Remove Repository abstract class references and any repository implementations
- [ ] Ensure DataManager is the single point of access for PostgreSQL in the diagram
- [ ] Update any documentation to reflect the new architecture

## 2. DataManager Core Enhancements
- [ ] Implement connection pooling directly in DataManager (HikariCP)
- [ ] Create proper transaction management functionality
- [ ] Add connection cleanup and resource management
- [ ] Set up database health checks accessible via metrics

## 3. Database Operations Implementation
- [ ] Move all PostgreSQL queries from repositories into DataManager methods
- [ ] Implement CRUD operations for all domain models
- [ ] Add batch operations capability for performance
- [ ] Create appropriate caching mechanisms for frequently accessed data

## 4. Method Signature Fixes
- [ ] Fix mapResultSetToEvent method signature and implementation
- [ ] Fix mapResultSetToUser method to properly map all fields
- [ ] Implement missing methods like getAllActiveEvents(), getUserFriends(), etc.
- [ ] Add proper ExecutionContext handling for async operations

## 5. External Component Updates
- [ ] Update Recommender to only use DataManager for data access
- [ ] Update Transformer to only use DataManager for data access
- [ ] Ensure event processing still works with the new architecture
- [ ] Fix any references to non-existent Repository objects

## 6. Testing Updates
- [ ] Refactor unit tests to mock DataManager instead of individual repositories
- [ ] Update integration tests that might have been using repositories directly
- [ ] Create comprehensive tests for DataManager CRUD operations
- [ ] Test transaction handling and failure scenarios

## 7. Error Handling & Monitoring
- [ ] Implement database-specific error handling in DataManager
- [ ] Add monitoring metrics for database operations
- [ ] Set up logging for all database operations
- [ ] Create backpressure mechanisms for heavy database load

## 8. Schema/Decoder Related Fixes
- [ ] Fix CirceJson decoders for all model classes
- [ ] Ensure consistency between database schema and model classes
- [ ] Implement proper mappings for all database columns

This approach keeps all PostgreSQL connections isolated to the DataManager class, which aligns with your simplified technical diagram where DataManager is the only component connecting to the database.