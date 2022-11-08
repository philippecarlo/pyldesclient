from dependency_injector import containers, providers
from services import LdesManager, LdesStore, LdesSyncer, LdesClient

class Container(containers.DeclarativeContainer):

    config = providers.Configuration(yaml_files=["config.yml"])

    ldes_manager_factory = providers.Factory(
        LdesManager,
        ldes_client = providers.Factory(LdesClient,),
        ldes_store = providers.Singleton(LdesStore,)
    )

    ldes_syncer_factory = providers.Factory(
        LdesSyncer,
        ldes_client = providers.Factory(LdesClient,),
        ldes_store = providers.Singleton(LdesStore,)
    )

