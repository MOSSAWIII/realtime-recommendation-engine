package repositories

/**
 * Documentation du mapping entre les tables SQL et les modèles Scala
 */
object TableMapping {
  
  /**
   * ## Table UTILISATEUR → Classe User
   * 
   * | Colonne SQL         | Type SQL    | Champ Scala       | Type Scala |
   * |--------------------|-------------|------------------|------------|
   * | ID_UTILISATEUR     | BIGINT      | userId           | Long       |
   * | NOM                | VARCHAR     | name             | String     |
   * | EMAIL              | VARCHAR     | email            | String     |
   * | LOCATION           | VARCHAR     | location         | String     |
   * | PREFERENCES        | JSONB       | preferences      | Map[String, String] |
   * 
   * Pour les préférences, nous utilisons circe pour la sérialisation/désérialisation JSON.
   */
  
  /**
   * ## Table EVENEMENT → Classe Event
   * 
   * | Colonne SQL         | Type SQL    | Champ Scala       | Type Scala |
   * |--------------------|-------------|------------------|------------|
   * | ID_EVENEMENT       | BIGINT      | eventId          | Long       |
   * | TITRE              | VARCHAR     | title            | String     |
   * | DESCRIPTION        | TEXT        | description      | String     |
   * | DATE_DEBUT         | TIMESTAMP   | startTime        | LocalDateTime |
   * | VILLE              | VARCHAR     | city             | String     |
   * | REGION             | VARCHAR     | region           | String     |
   * | PAYS               | VARCHAR     | country          | String     |
   * | c_1 ... c_100      | INT         | keywordVector    | Map[Int, Int] |
   * 
   * Notes:
   * - Les colonnes c_1 à c_100 représentent un vecteur de caractéristiques pour l'événement
   * - Ce vecteur est converti en Map[Int, Int] où la clé est l'indice (1-100) et la valeur est le poids
   */
  
  /**
   * ## Table PARTICIPATION_EVENEMENT → Classe EventParticipation
   * 
   * | Colonne SQL         | Type SQL    | Champ Scala       | Type Scala |
   * |--------------------|-------------|------------------|------------|
   * | ID_UTILISATEUR     | BIGINT      | userId           | Long       |
   * | ID_EVENEMENT       | BIGINT      | eventId          | Long       |
   * | STATUT             | VARCHAR     | status           | String     |
   * 
   * La classe EventParticipation est utilisée pour représenter la relation many-to-many
   * entre utilisateurs et événements.
   */
  
  /**
   * ## Table processed_events → Classe ProcessedEvent
   * 
   * | Colonne SQL         | Type SQL    | Champ Scala       | Type Scala |
   * |--------------------|-------------|------------------|------------|
   * | id                 | SERIAL      | id (Option)      | Option[Long] |
   * | user_id            | BIGINT      | userId           | Long       |
   * | event_type         | VARCHAR     | eventType        | String     |
   * | item_id            | BIGINT      | itemId           | Option[Long] |
   * | properties         | JSONB       | properties       | Map[String, String] |
   * | timestamp          | BIGINT      | timestamp        | Long       |
   * | processing_timestamp | BIGINT    | processingTimestamp | Long    |
   * 
   * Cette table stocke les événements utilisateur traités pour le système de recommandation.
   */
  
  /**
   * ## Table recommendations → Classe Recommendation
   * 
   * | Colonne SQL         | Type SQL    | Champ Scala       | Type Scala |
   * |--------------------|-------------|------------------|------------|
   * | id                 | SERIAL      | id               | Option[Long] |
   * | user_id            | BIGINT      | userId           | Long       |
   * | recommended_items  | JSONB       | recommendedItems | List[RecommendedItem] |
   * | score              | DOUBLE      | score            | Double     |
   * | timestamp          | BIGINT      | timestamp        | Long       |
   * 
   * Le champ recommended_items est sérialisé/désérialisé depuis/vers JSON avec circe.
   */
}

