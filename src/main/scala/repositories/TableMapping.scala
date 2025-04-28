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
   * | user_id            | BIGINT      | userId           | Long       |
   * | locale             | VARCHAR     | locale           | String     |
   * | birthyear          | INT         | birthYear        | Option[Int] |
   * | gender             | VARCHAR     | gender           | Option[String] |
   * | joinedat           | TIMESTAMP   | joinedAt         | Option[LocalDateTime] |
   * | location           | VARCHAR     | location         | Option[String] |
   * | timezone           | INT         | timezone         | Option[Int] |
   * | is_generated       | BOOLEAN     | isGenerated      | Boolean     |
   */

  /**
   * ## Table EVENEMENT → Classe Event
   * 
   * | Colonne SQL         | Type SQL    | Champ Scala       | Type Scala |
   * |--------------------|-------------|------------------|------------|
   * | event_id           | BIGINT      | eventId          | Long       |
   * | user_id            | BIGINT      | userId           | Long       |
   * | start_time         | TIMESTAMP   | startTime        | LocalDateTime |
   * | city               | VARCHAR     | city             | String     |
   * | state              | VARCHAR     | state            | Option[String] |
   * | zip                | VARCHAR     | zip              | Option[String] |
   * | country            | VARCHAR     | country          | String     |
   * | lat                | DOUBLE      | latitude         | Option[Double] |
   * | lng                | DOUBLE      | longitude        | Option[Double] |
   * | name               | VARCHAR     | name             | String     |
   * | description        | TEXT        | description      | Option[String] |
   * | c_1 ... c_100      | INT         | keywordVector    | Map[Int, Int] |
   * | c_other            | INT         | otherKeywords    | Option[Int] |
   */

  /**
   * ## Table PARTICIPATION_DETAIL → Classe ParticipationDetail
   * 
   * | Colonne SQL         | Type SQL    | Champ Scala       | Type Scala |
   * |--------------------|-------------|------------------|------------|
   * | event_id           | BIGINT      | eventId          | Long       |
   * | user_id            | BIGINT      | userId           | Long       |
   * | status             | VARCHAR     | status           | String     |
   */

  /**
   * ## Table RESEAU_AMIS → Classe FriendNetwork
   * 
   * | Colonne SQL         | Type SQL    | Champ Scala       | Type Scala |
   * |--------------------|-------------|------------------|------------|
   * | user_id            | BIGINT      | userId           | Long       |
   * | friends_list       | JSONB       | friendsList      | List[Long] |
   */

  /**
   * ## Table PARTICIPATION_EVENEMENT → Classe EventParticipation
   * 
   * | Colonne SQL         | Type SQL    | Champ Scala       | Type Scala |
   * |--------------------|-------------|------------------|------------|
   * | event_id           | BIGINT      | eventId          | Long       |
   * | yes_list           | JSONB       | yesList          | List[Long] |
   * | maybe_list         | JSONB       | maybeList        | List[Long] |
   * | invited_list       | JSONB       | invitedList      | List[Long] |
   * | no_list            | JSONB       | noList           | List[Long] |
   */

}

