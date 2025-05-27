package common.base

/**
 * Trait pour les tests nécessitant une base de données
 */
trait WithDatabase { self: BaseSpec =>
  // Méthodes pour configurer et nettoyer une base de données
}

/**
 * Trait pour les tests utilisant Docker
 */
trait WithDocker { self: BaseSpec =>
  // Méthodes pour gérer des conteneurs Docker
}

/**
 * Trait pour les tests isolés avec nettoyage de ressources
 */
trait WithIsolation { self: BaseSpec =>
  // Méthodes pour gérer l'isolation et le nettoyage
}

