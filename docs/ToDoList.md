# Tests unitaires du DataManager
./run-tests.sh specific:unit.application.core.DataManagerSpec [YES]

# Tests de health check du DataManager
./run-tests.sh specific:unit.application.core.DataManagerHealthCheckSpec [YES]

# Tests d'intégration de base de données du DataManager
./run-tests.sh specific:integration.db.DataManagerIntegrationSpec [YES]

# Tous les tests unitaires du core application (inclut DataManager)
./run-tests.sh unit:application [YES]

# Tous les tests d'intégration de base de données
./run-tests.sh integration:db [NOT YET]

# Tests spécifiques au composant DataManager
./run-tests.sh component:datamanager [YES]

# Tests marqués avec DatabaseTest (utilisent le DataManager)
./run-tests.sh tag:DatabaseTest

# Tests isolés (souvent liés aux composants core comme DataManager)
./run-tests.sh tag:Isolated

# Lance tous les tests liés au DataManager avec setup des containers
./run-tests.sh component:datamanager --debug


integration.db.DataManagerComprehensiveIntegrationSpec [YES]
integration.messaging.StreamingTransformerIntegrationSpec [YES]
integration.messaging.TransformerRecommenderMessagingSpec
integration.messaging.PipelineIntegrationSpec
integration.recommendation.TransformerRecommenderIntegrationSpec